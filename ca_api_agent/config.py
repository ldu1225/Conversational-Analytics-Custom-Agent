# ca_api_agent/config.py
# =====================================================================
# USER CONFIGURATION: Change these variables to deploy your agent
# =====================================================================

# 1. Google Cloud Settings
PROJECT_ID = "your-gcp-project-id"
LOCATION = "us-central1"  # Region for Reasoning Engine (Agent Engine) deployment and CA API

# 2. BigQuery Tables Settings
# List all specific tables you want your agent to reference.
# The agent will dynamically discover and query only these tables.
TABLES = [
    {"dataset_id": "your_dataset_id", "table_id": "your_table_id_1"},
    {"dataset_id": "your_dataset_id", "table_id": "your_table_id_2"}
]

# 3. Agent Display Settings
AGENT_DISPLAY_NAME = "Enterprise BQ Logs Analyst Agent"
DATA_AGENT_NAME = "your-data-agent-name"

# 4. Agent Persona / System Instruction
# Dynamic, generic prompt without hardcoded table names.
# Prompts the SQL generator to semantically UNION/JOIN tables whenever relevant.
SYSTEM_INSTRUCTION = (
    "당신은 전문 기업용 BigQuery 데이터 분석가 에이전트입니다. "
    "반드시 정중하고 친절한 한국어로 질문에 답변해야 합니다. "
    "제공된 BigQuery 테이블들을 최대한 활용하여 사용자의 질문에 성실하게 답변하세요. "
    "여러 테이블에 걸쳐 연관성이 있거나 상호 보완적인 데이터가 존재할 경우, "
    "필요시 자동으로 이를 결합(UNION 또는 JOIN)하여 한 곳에서 통합하여 산출하는 정확한 SQL을 생성해 실행해야 합니다. "
    "절대 특정 테이블 하나에만 국한되거나 임의로 데이터를 누락시키지 말고, 사용자의 비즈니스 분석 질문에 가장 부합하는 종합적인 결과를 표와 텍스트 요약 형식으로 도출하세요. "
    "차트는 직접 텍스트로 그리지 말고 필요시 파이썬 시각화 도구를 활용할 수 있도록 유도하세요."
)
