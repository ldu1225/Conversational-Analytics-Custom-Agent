# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

"""Conversational Analytics query agent and CA API streaming bridge with dynamic BigQuery table resolution."""

import os
os.environ["GRPC_ENABLE_FORK_SUPPORT"] = "true"
import asyncio
import json
import logging
import os
from typing import Any, AsyncGenerator

from dotenv import load_dotenv
from google.adk.agents import BaseAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.events import Event
from google.api_core import exceptions as api_exceptions
from google.cloud import geminidataanalytics_v1alpha as geminidataanalytics
from google.cloud import bigquery
from google.genai import Client, types
from google.protobuf import json_format
from typing_extensions import override
from .. import config

import io
import base64
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

load_dotenv()
logger = logging.getLogger(__name__)

DATA_RESULT_STATE_KEY = "temp:data_result"
SUMMARY_STATE_KEY = "temp:summary_data"
DATA_MESSAGE_DISPLAY_MAX_ROWS = 5
DATA_TABLE_DISPLAY_MAX_ROWS = 50


def _generate_chart_png_base64(plain_rows: list[dict[str, Any]]) -> str | None:
    """Generates an adaptive, highly visual matplotlib chart (line trend or bar chart) in memory and returns its base64 PNG string."""
    if not plain_rows or len(plain_rows) < 2:
        return None

    try:
        # 1. Semantically identify columns
        keys = list(plain_rows[0].keys())
        date_col, cat_col, metric_col = None, None, None

        date_keywords = ('date', 'timestamp', 'day', 'week', 'month', 'year', 'time')

        for key in keys:
            val = plain_rows[0][key]
            key_lower = key.lower()
            
            # Detect Date columns
            if any(keyword in key_lower for keyword in date_keywords):
                date_col = key
            # Detect Metric columns
            elif isinstance(val, (int, float)):
                metric_col = key
            # Detect Category columns
            elif isinstance(val, str):
                cat_col = key

        # Fallbacks if semantic detection is incomplete
        if not metric_col:
            # Find any numeric column
            for key in keys:
                if isinstance(plain_rows[0][key], (int, float)):
                    metric_col = key
                    break

        if not metric_col:
            return None # Cannot plot without a metric

        plt.figure(figsize=(8, 4.5), dpi=100)

        # CASE 1: Multi-Category Line Trend (e.g. Top 3 users daily usage trend!)
        if date_col and cat_col and metric_col:
            logger.info("Plotting Multi-Category Line Trend: %s and %s over %s", metric_col, cat_col, date_col)
            
            # Group data by category
            categories = set(row.get(cat_col) for row in plain_rows if row.get(cat_col))
            # Limit to top 5 categories for visual clarity
            categories = list(categories)[:5]

            # Sort plain_rows by date to ensure line goes left-to-right chronologically
            sorted_rows = sorted(plain_rows, key=lambda r: str(r.get(date_col, "")))

            for cat in categories:
                cat_rows = [r for r in sorted_rows if r.get(cat_col) == cat]
                dates = [str(r.get(date_col, ""))[:10] for r in cat_rows]  # Truncate timestamps to YYYY-MM-DD
                metrics = [float(r.get(metric_col, 0)) for r in cat_rows]
                
                plt.plot(dates, metrics, marker='o', label=str(cat)[:25], linewidth=2)

            plt.legend(title=cat_col, bbox_to_anchor=(1.05, 1), loc='upper left')

        # CASE 2: Simple Line Trend (e.g. Daily activity count trend!)
        elif date_col and metric_col:
            logger.info("Plotting Simple Line Trend: %s over %s", metric_col, date_col)
            sorted_rows = sorted(plain_rows, key=lambda r: str(r.get(date_col, "")))
            dates = [str(r.get(date_col, ""))[:10] for r in sorted_rows]
            metrics = [float(r.get(metric_col, 0)) for r in sorted_rows]
            
            plt.plot(dates, metrics, marker='o', color='#1a73e8', linewidth=2, label=metric_col)
            plt.legend(loc='upper right')

        # CASE 3: Standard Bar Chart (Default comparison!)
        else:
            x_col = cat_col or date_col or keys[0]
            logger.info("Plotting Bar Chart: %s by %s", metric_col, x_col)
            
            display_rows = plain_rows[:15] # Limit to top 15 columns
            x_data = [str(row.get(x_col, ""))[:25] for row in display_rows]
            y_data = [float(row.get(metric_col, 0)) for row in display_rows]

            bars = plt.bar(x_data, y_data, color='#1a73e8', edgecolor='#185abc', alpha=0.9, label=metric_col)

            # Add labels on top of the bars
            for bar in bars:
                height = bar.get_height()
                plt.annotate(f'{height:,.0f}',
                             xy=(bar.get_x() + bar.get_width() / 2, height),
                             xytext=(0, 3),
                             textcoords="offset points",
                             ha='center', va='bottom', fontsize=8, color='#3c4043')
            plt.legend(loc='upper right')

        # Clean grid & axis styling
        plt.title(f"{metric_col} Visualization Chart", fontsize=12, pad=15, fontweight='bold', color='#202124')
        plt.xlabel(date_col or cat_col or "Dimension", fontsize=10, labelpad=10, color='#5f6368')
        plt.ylabel(metric_col, fontsize=10, labelpad=10, color='#5f6368')
        plt.xticks(rotation=45, ha='right', fontsize=8, color='#5f6368')
        plt.yticks(fontsize=8, color='#5f6368')
        plt.grid(axis='y', linestyle='--', alpha=0.5)
        plt.tight_layout()

        # Save to in-memory buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight')
        plt.close()
        buf.seek(0)

        return base64.b64encode(buf.read()).decode('ascii')
    except Exception as e:
        logger.exception("Failed to generate Matplotlib chart: %s", e)
        return None



def _message_to_dict(message: Any) -> dict[str, Any]:
    proto_message = getattr(message, "_pb", message)
    return json_format.MessageToDict(
        proto_message,
        preserving_proto_field_name=True,
    )


def _to_plain_rows(rows: list[Any]) -> list[dict[str, Any]]:
    plain_rows: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            plain_rows.append(row)
            continue
        if hasattr(row, "items"):
            try:
                item_dict = dict(row.items())
                if item_dict:
                    plain_rows.append(item_dict)
                    continue
            except Exception:
                pass
        try:
            row_dict = _message_to_dict(row)
        except Exception:
            row_dict = {}
        if isinstance(row_dict, dict):
            if set(row_dict.keys()) == {"fields"} and isinstance(
                row_dict["fields"], dict
            ):
                row_dict = row_dict["fields"]
            if row_dict:
                plain_rows.append(row_dict)
                continue
        plain_rows.append({"value": str(row)})
    return plain_rows


def _truncate_data_message_for_display(data_message: dict[str, Any]) -> dict[str, Any]:
    result = data_message.get("result")
    if not isinstance(result, dict):
        return data_message

    display_data_message = dict(data_message)
    display_result = dict(result)
    trimmed_row_counts: dict[str, int] = {}

    for field_name in ("data", "formatted_data"):
        rows = result.get(field_name)
        if not isinstance(rows, list):
            continue
        if len(rows) <= DATA_MESSAGE_DISPLAY_MAX_ROWS:
            continue
        display_result[field_name] = rows[:DATA_MESSAGE_DISPLAY_MAX_ROWS]
        trimmed_row_counts[field_name] = len(rows) - DATA_MESSAGE_DISPLAY_MAX_ROWS

    if not trimmed_row_counts:
        return data_message

    display_result["display_trimmed_row_counts"] = trimmed_row_counts
    display_data_message["result"] = display_result
    return display_data_message


def _build_data_message_trim_notice(display_data_message: dict[str, Any]) -> str | None:
    result = display_data_message.get("result")
    if not isinstance(result, dict):
        return None

    trimmed_row_counts = result.get("display_trimmed_row_counts")
    if not isinstance(trimmed_row_counts, dict) or not trimmed_row_counts:
        return None

    trimmed_fields = ", ".join(
        f"{field}: {count} row(s) omitted"
        for field, count in trimmed_row_counts.items()
    )
    return (
        f"_DataMessage JSON was trimmed to {DATA_MESSAGE_DISPLAY_MAX_ROWS} rows per field "
        f"({trimmed_fields})._\n"
    )


def _format_code_block_json(payload: dict[str, Any]) -> str:
    return f"```json\n{json.dumps(payload, indent=2)}\n```\n"


def _stringify_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def _escape_markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def _format_simple_markdown_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "_No rows returned._\n"

    headers: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in headers:
                headers.append(key)

    if not headers:
        return "_No tabular columns returned._\n"

    header_line = "| " + " | ".join(headers) + " |"
    separator_line = "| " + " | ".join(["---"] * len(headers)) + " |"

    body_lines: list[str] = []
    for row in rows:
        cells = [
            _escape_markdown_cell(_stringify_cell(row.get(header)))
            for header in headers
        ]
        body_lines.append("| " + " | ".join(cells) + " |")

    return "\n".join([header_line, separator_line, *body_lines]) + "\n"


def _clear_response_shape_state(ctx: InvocationContext) -> None:
    """Clears routing-related state to avoid stale data across turns."""
    keys_to_clear = (
        DATA_RESULT_STATE_KEY,
        SUMMARY_STATE_KEY,
    )
    for key in keys_to_clear:
        ctx.session.state.pop(key, None)


def _build_inline_context() -> geminidataanalytics.Context:
    project_id = config.PROJECT_ID
    logger.info("Configuring BigQuery tables from config.TABLES for project: %s", project_id)
    
    table_references = []
    for table_conf in config.TABLES:
        dataset_id = table_conf["dataset_id"]
        table_id = table_conf["table_id"]
        logger.info("Registering table: %s.%s.%s", project_id, dataset_id, table_id)
        table_references.append(
            geminidataanalytics.BigQueryTableReference(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id
            )
        )

    logger.info("Total registered tables: %d", len(table_references))

    return geminidataanalytics.Context(
        system_instruction=config.SYSTEM_INSTRUCTION,
        datasource_references=geminidataanalytics.DatasourceReferences(
            bq=geminidataanalytics.BigQueryTableReferences(table_references=table_references)
        ),
    )


async def stream_nlq(question: str, ctx: InvocationContext) -> AsyncGenerator[str, None]:
    """Streams CA API responses and persists structured result data to session state."""
    logger.info({"request": question})
    _clear_response_shape_state(ctx)

    project = (
        f"projects/{os.getenv('GOOGLE_CLOUD_PROJECT')}"
        f"/locations/global"
    )

    user_message = geminidataanalytics.Message(
        user_message=geminidataanalytics.UserMessage(text=question)
    )
    request_messages = [user_message]

    conversation_id = ctx.session.id
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    conversation_path = f"projects/{project_id}/locations/global/conversations/{conversation_id}"

    client = geminidataanalytics.DataChatServiceAsyncClient()

    # 1. Stateful Chat: Try to get or create a persistent conversation resource on GCP
    try:
        await client.get_conversation(name=conversation_path)
        logger.info("Session conversation %s already exists.", conversation_id)
    except Exception:
        logger.info("Session conversation %s does not exist. Creating...", conversation_id)
        try:
            new_conv = geminidataanalytics.Conversation(
                agents=[f"projects/{project_id}/locations/global/dataAgents/{config.DATA_AGENT_NAME}"]
            )
            await client.create_conversation(
                parent=f"projects/{project_id}/locations/global",
                conversation_id=conversation_id,
                conversation=new_conv
            )
            logger.info("Successfully created persistent conversation on Google Cloud.")
        except Exception as e:
            logger.warning("Failed to create conversation resource: %s", e)

    # 2. Build ChatRequest with ConversationReference for multi-turn tracking
    chat_request = geminidataanalytics.ChatRequest(
        parent=project,
        messages=request_messages,
        conversation_reference=geminidataanalytics.ConversationReference(
            conversation=conversation_path
        ),
        inline_context=_build_inline_context(),
    )

    yielded_sql = False
    yielded_table = False
    yielded_chart = False

    # Save generated_sql in local scope to pass to interpreter later
    captured_sql = ""

    try:
        stream = await client.chat(request=chat_request)

        async for response in stream:
            if not response.system_message:
                continue

            system_message = response.system_message
            if system_message.data:
                data_node = system_message.data
                data_message = _message_to_dict(data_node)

                # Hiding raw technical JSON dumps! We only show clean client-facing info.
                logger.debug("Received CA stream data node: %s", data_message)

                query_payload = data_message.get("query")
                result_payload = data_message.get("result")

                # A. Display generated SQL cleanly to client ONCE!
                generated_sql = data_message.get("generated_sql")
                if generated_sql and not yielded_sql:
                    captured_sql = generated_sql
                    yield f"### 🔍 생성된 SQL 쿼리\n```sql\n{generated_sql.strip()}\n```\n"
                    yielded_sql = True
                    yielded_any = True
                    await asyncio.sleep(0)

                if isinstance(result_payload, dict) and not yielded_table:
                    payload_rows = result_payload.get("data")
                    plain_rows = (
                        _to_plain_rows(payload_rows)
                        if isinstance(payload_rows, list)
                        else []
                    )
                    if not plain_rows:
                        result = getattr(data_node, "result", None)
                        raw_rows = list(result.data) if result and result.data else []
                        plain_rows = _to_plain_rows(raw_rows)
                    ctx.session.state[DATA_RESULT_STATE_KEY] = plain_rows

                    # B. Display clean markdown table cleanly to client ONCE!
                    yield "### 📊 데이터 결과 분석\n"
                    yield _format_simple_markdown_table(
                        plain_rows[:DATA_TABLE_DISPLAY_MAX_ROWS]
                    )
                    if len(plain_rows) > DATA_TABLE_DISPLAY_MAX_ROWS:
                        yield (
                            f"\n_상위 {DATA_TABLE_DISPLAY_MAX_ROWS}개 행만 표시 중입니다 (총 {len(plain_rows)}개 행)._\n"
                        )
                    yielded_table = True
                    yielded_any = True
                    await asyncio.sleep(0)

            # 3. Chart Generation: Capture native vega_config requests and render beautiful matplotlib plots directly to base64 ONCE!
            if system_message.chart and not yielded_chart:
                logger.info("Conversational Analytics requested chart. Rendering plot natively...")
                plain_rows = ctx.session.state.get(DATA_RESULT_STATE_KEY)
                if plain_rows:
                    b64_chart = _generate_chart_png_base64(plain_rows)
                    if b64_chart:
                        yield f"\n### 📈 데이터 시각화 차트\n![chart](data:image/png;base64,{b64_chart})\n"
                        yielded_chart = True
                        yielded_any = True
                        await asyncio.sleep(0)

        # 4. Professional Fallback Data Interpretation:
        # If a query successfully ran, invoke Gemini 3.5 Flash to output deep client-facing insights!
        plain_rows = ctx.session.state.get(DATA_RESULT_STATE_KEY)
        if plain_rows:
            logger.info("Invoking Gemini 3.5 Flash for deep analytical data interpretation...")
            try:
                genai_client = Client(
                    vertexai=True,
                    project=os.getenv("GOOGLE_CLOUD_PROJECT"),
                    location="global"
                )
                # Format table to string to feed as context
                table_str = _format_simple_markdown_table(plain_rows[:15])
                
                interpret_prompt = (
                    f"사용자 질문: {question}\n"
                    f"실행된 SQL:\n{captured_sql}\n"
                    f"조회 결과 데이터:\n{table_str}\n"
                )
                
                response = await genai_client.aio.models.generate_content(
                    model="gemini-3.5-flash",
                    contents=interpret_prompt,
                    config=types.GenerateContentConfig(
                        system_instruction=(
                            "당신은 시니어 엔터프라이즈 데이터 분석 리드입니다. "
                            "사용자가 요청한 데이터 분석 결과를 면밀히 해독하고, 고객에게 친절하고 정중한 한국어로 상세 비즈니스 보고서를 작성해야 합니다. "
                            "반드시 답변 구조에 다음 요소를 포함시키세요:\n"
                            "1. 분석 결과의 핵심 발견점을 친절히 설명하는 인트로 문장.\n"
                            "2. '주요 인사이트' 라는 제목의 명확한 단락을 구성하고, 데이터 결과에 기반하여 세밀한 통계 분석, 사용자 분배율, 활동량 격차의 원인, 혹은 시간적 추이 분석 등의 핵심 비즈니스 발견점을 구체적이고 정연한 글머리 기호(Bullet points) 목록으로 상세히 요약하여 서술하세요.\n"
                            "3. 수치적 통계를 그대로 인용하며 신뢰도 높은 컨설팅 톤앤매너를 유지하세요."
                        )
                    )
                )
                yield f"\n{response.text.strip()}\n"
                yielded_any = True
            except Exception as interpret_err:
                logger.exception("Failed to generate custom interpretation: %s", interpret_err)



        if not yielded_any:
            logger.info("Conversational Analytics API returned empty stream. Invoking Gemini chat fallback...")
            try:
                genai_client = Client(
                    vertexai=True,
                    project=os.getenv("GOOGLE_CLOUD_PROJECT"),
                    location="global",
                )
                response = await genai_client.aio.models.generate_content(
                    model="gemini-3.5-flash",
                    contents=question,
                    config=types.GenerateContentConfig(
                        system_instruction=(
                            "당신은 전문 엔터프라이즈 데이터 분석가 에이전트입니다. "
                            "반드시 정중하고 친절한 한국어로 답변해야 합니다. "
                            "사용자의 일상적인 인사말, 안부 묻기, 자기소개 요청, 일반 상식 또는 비즈니스에 관한 가벼운 수다(Chit-chat)에 대해 자연스럽고 다정하게 답변을 하세요. "
                            "만약 사용자의 입력이 데이터 분석이나 지표 조회를 요청하는 것처럼 보인다면, "
                            "어떤 구체적인 데이터를 보고 싶으신지 되묻거나, 빅쿼리 데이터베이스에 연결된 테이블에 관한 구체적인 질문을 입력해 달라고 친절하게 유도하세요."
                        )
                    )
                )
                yield response.text + "\n"
            except Exception as chat_err:
                logger.error("Gemini chat fallback failed: %s", chat_err)
                yield "죄송합니다. 빅쿼리 데이터셋과 관련이 없거나 현재 분석할 수 없는 질문입니다. 빅쿼리 데이터베이스의 컬럼이나 테이블 데이터에 관한 구체적인 질문을 입력해 주세요. (예: '컬럼별 데이터 건수 보여줘')\n"
    except api_exceptions.GoogleAPICallError as err:
        code_fn = getattr(err, "code", None)
        code = code_fn() if callable(code_fn) else None
        error_code = str(getattr(code, "name", "UNKNOWN"))
        error_message = str(getattr(err, "message", str(err)))
        logger.error("Error from CA API: %s - %s", error_code, error_message)
        yield json.dumps({"error": error_message, "code": error_code})
    except Exception as err:  # pragma: no cover - defensive catch for service errors
        logger.exception("Unexpected error from CA API")
        yield json.dumps({"error": str(err)})


def _extract_user_question(ctx: InvocationContext) -> str | None:
    """Extracts plain-text question from the invocation's user content."""
    user_content = ctx.user_content
    if not user_content or not user_content.parts:
        return None

    text_parts: list[str] = []
    for part in user_content.parts:
        text = getattr(part, "text", None)
        if isinstance(text, str) and text.strip():
            text_parts.append(text.strip())

    if not text_parts:
        return None
    return "\n".join(text_parts)


class ConversationalAnalyticsQueryAgent(BaseAgent):
    """Runs NLQ requests against CA API and streams results back into the chat."""

    name: str
    description: str = ""

    def __init__(
        self,
        name: str,
        description: str = "",
    ) -> None:
        init_data: dict[str, Any] = {
            "name": name,
            "description": description,
        }
        super().__init__(**init_data)

    @override
    async def _run_async_impl(
        self, ctx: InvocationContext
    ) -> AsyncGenerator[Event, None]:
        question = _extract_user_question(ctx)
        if not question:
            error_content = types.Content(
                role="model",
                parts=[
                    types.Part(
                        text=(
                            "I couldn't read your question. "
                            "Please send a text prompt."
                        )
                    )
                ],
            )
            yield Event(
                author=self.name,
                partial=False,
                turn_complete=True,
                invocation_id=ctx.invocation_id,
                content=error_content,
            )
            return

        status_message = types.Content(
            role="model",
            parts=[types.Part(text="Invoking the Conversational Analytics API...")],
        )
        yield Event(
            author=self.name,
            partial=False,
            turn_complete=False,
            invocation_id=ctx.invocation_id,
            content=status_message,
        )

        async for data_chunk in stream_nlq(question, ctx):
            chunk_content = types.Content(
                role="model",
                parts=[types.Part(text=data_chunk)],
            )
            yield Event(
                author=self.name,
                partial=False,
                turn_complete=False,
                invocation_id=ctx.invocation_id,
                content=chunk_content,
            )

        yield Event(
            author=self.name,
            partial=False,
            turn_complete=True,
            invocation_id=ctx.invocation_id,
        )


def build_conversational_analytics_query_agent(
    name: str = "conversational_analytics_query_agent",
) -> ConversationalAnalyticsQueryAgent:
    """Factory for the CA query sub-agent."""
    return ConversationalAnalyticsQueryAgent(
        name=name,
        description="Always forwards each user request directly to the Conversational Analytics API.",
    )
