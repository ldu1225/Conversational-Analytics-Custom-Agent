# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

"""Root agent for deterministic CA-first orchestration with optional sub-agents."""

import base64
import logging
import re
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, cast

from google.adk.agents import BaseAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.events import Event
from google.genai import types
from pydantic import PrivateAttr
from typing_extensions import override

from .agents import (
    DATA_RESULT_STATE_KEY,
    build_conversational_analytics_query_agent,
    build_visualization_agent,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OptionalSubAgentSpec:
    """Configuration for deterministic optional sub-agent routing."""

    key: str
    description: str
    agent: BaseAgent
    run_when: Callable[[InvocationContext], bool]


class RootAgent(BaseAgent):
    """Always runs CA first, then conditionally runs optional sub-agents."""

    name: str
    description: str = ""
    query_agent: BaseAgent
    _optional_sub_agents: list[OptionalSubAgentSpec] = PrivateAttr(default_factory=list)

    def __init__(
        self,
        name: str,
        description: str,
        query_agent: BaseAgent,
        optional_sub_agents: list[OptionalSubAgentSpec],
    ) -> None:
        init_data: dict[str, Any] = {
            "name": name,
            "description": description,
            "query_agent": query_agent,
        }
        super().__init__(**init_data)
        self._optional_sub_agents = optional_sub_agents

    @staticmethod
    def _has_non_empty_state(ctx: InvocationContext, key: str) -> bool:
        value = ctx.session.state.get(key)
        if value is None:
            return False
        if isinstance(value, (str, bytes, list, tuple, set, dict)):
            return len(value) > 0
        return bool(value)

    def _select_optional_sub_agents(
        self, ctx: InvocationContext
    ) -> list[OptionalSubAgentSpec]:
        selected: list[OptionalSubAgentSpec] = []

        for spec in self._optional_sub_agents:
            try:
                if spec.run_when(ctx):
                    selected.append(spec)
            except Exception:  # pragma: no cover - defensive route guard
                logger.exception("Failed while evaluating routing rule for '%s'.", spec.key)
        return selected

    @staticmethod
    def _as_non_terminal(event: Event) -> Event:
        """Returns a copy of the event marked as non-terminal for orchestration streaming."""
        if not event.turn_complete:
            return event
        copy_fn = getattr(event, "model_copy", None)
        if callable(copy_fn):
            return cast(Event, copy_fn(update={"turn_complete": False}))
        event.turn_complete = False
        return event

    @staticmethod
    def _strip_code_blocks(text: str) -> str:
        return re.sub(r"```.*?```", "", text, flags=re.DOTALL)

    @staticmethod
    async def _sanitize_visualization_content(
        content: types.Content | None,
        ctx: InvocationContext,
    ) -> types.Content | None:
        if not content or not content.parts:
            return content

        sanitized_parts: list[types.Part] = []
        for part in content.parts:
            # 1. Check if inline_data is already present
            inline_data = getattr(part, "inline_data", None)
            if inline_data and inline_data.data:
                mime_type = getattr(inline_data, "mime_type", None) or "image/png"
                if mime_type.startswith("image/"):
                    b64_data = base64.b64encode(inline_data.data).decode("ascii")
                    sanitized_parts.append(
                        types.Part(text=f"![chart](data:{mime_type};base64,{b64_data})")
                    )
                    continue

            # 2. If inline_data is not present, check if it's a text reference to a saved artifact
            text = getattr(part, "text", None)
            if isinstance(text, str) and text.strip():
                # Match "Saved as artifact: <filename>.png"
                match = re.search(r"Saved as artifact:\s*(\S+\.png)", text)
                if match and ctx.artifact_service:
                    filename = match.group(1)
                    logger.info("Intercepted saved artifact reference: %s. Loading from service...", filename)
                    try:
                        # Load the artifact from the service
                        artifact_part = await ctx.artifact_service.load_artifact(
                            app_name=ctx.app_name,
                            user_id=ctx.user_id,
                            session_id=ctx.session.id,
                            filename=filename
                        )
                        if artifact_part and artifact_part.inline_data and artifact_part.inline_data.data:
                            img_data = artifact_part.inline_data.data
                            mime_type = artifact_part.inline_data.mime_type or "image/png"
                            b64_data = base64.b64encode(img_data).decode("ascii")
                            
                            # Replace "Saved as artifact" with the rendered image markdown
                            image_markdown = f"![chart](data:{mime_type};base64,{b64_data})\n"
                            
                            # We also strip the leaked thought block if present in the text
                            remaining_text = text.replace(match.group(0), "").strip()
                            # Remove leaked thoughts starting with "thought"
                            remaining_text = re.sub(r"(?i)thought\s+.*", "", remaining_text).strip()
                            
                            full_response = image_markdown
                            if remaining_text:
                                full_response += f"\n{remaining_text}"
                                
                            sanitized_parts.append(types.Part(text=full_response))
                            continue
                    except Exception as e:
                        logger.error("Failed to load and inline artifact image %s: %s", filename, e)

                # If not an artifact reference, just strip code blocks and clean up thought leaks
                cleaned = RootAgent._strip_code_blocks(text).strip()
                cleaned = re.sub(r"(?i)thought\s+.*", "", cleaned).strip()
                if cleaned:
                    sanitized_parts.append(types.Part(text=cleaned))

        if not sanitized_parts:
            return None
        return types.Content(role=content.role, parts=sanitized_parts)

    @override
    async def _run_async_impl(
        self, ctx: InvocationContext
    ) -> AsyncGenerator[Event, None]:
        async for event in self.query_agent.run_async(ctx):
            yield self._as_non_terminal(event)

        selected_agents = self._select_optional_sub_agents(ctx)
        if selected_agents:
            logger.info(
                "Optional sub-agents selected: %s",
                ", ".join(spec.key for spec in selected_agents),
            )
        else:
            logger.info("No optional sub-agents selected for this request.")

        for spec in selected_agents:
            status_content = types.Content(
                role="model",
                parts=[types.Part(text=f"Running optional step: {spec.description}")],
            )
            yield Event(
                author=self.name,
                partial=False,
                turn_complete=False,
                invocation_id=ctx.invocation_id,
                content=status_content,
            )

            try:
                async for event in spec.agent.run_async(ctx):
                    forward_event = event
                    if spec.key == "visualization":
                        sanitized_content = await self._sanitize_visualization_content(
                            event.content, ctx
                        )
                        if sanitized_content is None:
                            continue
                        copy_fn = getattr(event, "model_copy", None)
                        if callable(copy_fn):
                            forward_event = cast(
                                Event, copy_fn(update={"content": sanitized_content})
                            )
                        else:
                            event.content = sanitized_content
                            forward_event = event
                    yield self._as_non_terminal(forward_event)
            except Exception as err:  # pragma: no cover - defensive runtime guard
                logger.exception("Optional sub-agent '%s' failed.", spec.key)
                error_content = types.Content(
                    role="model",
                    parts=[
                        types.Part(
                            text=(
                                f"Optional step '{spec.key}' failed with error: {err}. "
                                "Continuing with available results."
                              )
                        )
                    ],
                )
                yield Event(
                    author=self.name,
                    partial=False,
                    turn_complete=False,
                    invocation_id=ctx.invocation_id,
                    content=error_content,
                )

        yield Event(
            author=self.name,
            partial=False,
            turn_complete=True,
            invocation_id=ctx.invocation_id,
        )


data_query_agent = build_conversational_analytics_query_agent()

root_agent = RootAgent(
    name="root_agent",
    description=(
        "Top-level deterministic root agent. It always runs the CA query agent."
    ),
    query_agent=data_query_agent,
    optional_sub_agents=[],
)

__all__ = ["root_agent"]
