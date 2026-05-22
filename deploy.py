# deploy.py
# Automates deployment of the ADK agent using settings from config.py

import subprocess
import sys
import os

# Add path to find config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from ca_api_agent import config

def deploy():
    print("Starting ADK deployment to Agent Engine...")
    print(f"Project: {config.PROJECT_ID}")
    print(f"Region: {config.LOCATION}")
    print(f"Display Name: {config.AGENT_DISPLAY_NAME}")
    
    adk_bin = "/Library/Frameworks/Python.framework/Versions/3.12/bin/adk"
    if not os.path.exists(adk_bin):
        adk_bin = "adk"  # Fallback to global path
        
    command = [
        adk_bin, "deploy", "agent_engine",
        f"--project={config.PROJECT_ID}",
        f"--region={config.LOCATION}",
        f"--display_name={config.AGENT_DISPLAY_NAME}",
        "ca_api_agent"
    ]
    
    print(f"Running command: {' '.join(command)}")
    try:
        # Run setup env script first to make sure .env is in sync
        subprocess.run(["python3", "setup_env.py"], check=True)
        
        # Run deployment
        result = subprocess.run(command, check=True)
        if result.returncode == 0:
            print("\nDeployment completed successfully!")
        else:
            print(f"\nDeployment failed with status: {result.returncode}")
    except Exception as e:
        print(f"\nError running deployment: {e}")

if __name__ == "__main__":
    deploy()
