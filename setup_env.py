# setup_env.py
# Generates .env file from config.py dynamically

from ca_api_agent import config

env_content = f"""GOOGLE_CLOUD_PROJECT={config.PROJECT_ID}
GOOGLE_CLOUD_LOCATION={config.LOCATION}
BIGQUERY_PROJECT={config.PROJECT_ID}
"""

with open(".env", "w") as f:
    f.write(env_content)
print("Successfully generated .env file from config.py!")
