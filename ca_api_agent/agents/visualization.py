# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

"""Factory for optional visualization sub-agent."""

from google.adk.agents import LlmAgent
from google.adk.code_executors import BuiltInCodeExecutor
from google.adk.planners.built_in_planner import BuiltInPlanner
from google.genai.types import ThinkingConfig


def build_visualization_agent() -> LlmAgent:
    """Creates the optional visualization sub-agent."""
    return LlmAgent(
        name="visualization_render_agent",
        model="gemini-3.5-flash",
        description=(
            "Creates plots from query data using Python, pandas, and matplotlib."
        ),
        instruction="""You are a data visualization expert. Your primary purpose is to create insightful and clear plots from data.
When you receive a request with data, your task is to:
1. Understand the data and the user's request for visualization.
2. Write Python code using pandas to prepare the data and matplotlib to generate a plot.
3. Keep chart size constrained to fit chat UI: use matplotlib figure width <= 8 inches and dpi <= 100.
4. Ensure your code is self-contained and generates a visual output, for example by calling plt.show().
5. All text inside the chart plot itself (such as title, x-axis label, y-axis label, legend, ticks) MUST be strictly written in English to avoid character rendering/glitch issues. Never use Korean or non-ASCII characters in any chart elements.
6. Along with the code that generates the plot, provide a brief, one-sentence summary of what the plot shows. You MUST write this summary in Korean.

The code will be executed and the resulting plot will be displayed.
Here is the data: {temp:data_result}""",
        code_executor=BuiltInCodeExecutor(),
    )
