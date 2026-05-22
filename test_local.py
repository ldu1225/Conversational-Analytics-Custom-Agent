# test_local.py
# Test the ADK BQ logs agent programmatically

import asyncio
import sys
import os
from ca_api_agent.root_agent import root_agent
from google.adk.agents import InvocationContext, RunConfig
from google.adk.sessions import Session, InMemorySessionService
from google.adk.artifacts.in_memory_artifact_service import InMemoryArtifactService
from google.genai import types

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def main():
    print("Starting local test for BQ Conversational Analytics agent...")
    print(f"Configured BigQuery dataset: {os.getenv('BIGQUERY_PROJECT')}.{os.getenv('BIGQUERY_DATASET')}")
    
    # Initialize a mock ADK InvocationContext
    # We use a simple question that can be answered by the BQ log schema
    question = "Show me the count of activities by method name"
    if len(sys.argv) > 1:
        question = sys.argv[1]
        
    print(f"Question: '{question}'")
    
    artifact_service = InMemoryArtifactService()
    
    ctx = InvocationContext(
        invocation_id="test_inv_001",
        user_content=types.Content(
            role="user",
            parts=[types.Part(text=question)]
        ),
        session=Session(
            id="test_session_001",
            app_name="test_app",
            user_id="test_user"
        ),
        session_service=InMemorySessionService(),
        artifact_service=artifact_service,
        agent=root_agent,
        run_config=RunConfig()
    )
    
    try:
        async for event in root_agent.run_async(ctx):
            # Format event printing
            author = event.author
            turn_complete = event.turn_complete
            content = event.content
            
            if content and content.parts:
                for part in content.parts:
                    text = getattr(part, "text", None)
                    if text:
                        print(f"[{author}]: {text}")
            
            if turn_complete:
                print(f"--- Turn Complete for {author} ---")
                
    except Exception as e:
         print(f"\nError during agent execution: {e}")
         import traceback
         traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
