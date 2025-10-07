import requests
import json
from typing import Dict, Any, Optional

# --- Configuration (Must be updated for your environment) ---
AIRFLOW_HOST = "http://localhost:8080"
AIRFLOW_API_ENDPOINT = f"{AIRFLOW_HOST}/api/v1/dags"

# Assuming you set up an API token or basic authentication
AUTH_HEADERS = {
    "Authorization": "Basic YWlyZmxvdzphaXJmbG93",  # Base64 encoded 'airflow:airflow'
    "Content-Type": "application/json",
    "Accept": "application/json"
}


def trigger_airflow_dag(dag_id: str, run_config: Dict[str, Any]) -> Optional[requests.Response]:
    """
    Triggers an Airflow DAG run using the REST API.
    """
    url = f"{AIRFLOW_API_ENDPOINT}/{dag_id}/dagRuns"

    payload = {
        "conf": run_config,
        "execution_date": None
    }

    try:
        response = requests.post(url, headers=AUTH_HEADERS, data=json.dumps(payload))
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error triggering Airflow DAG {dag_id}: {e}")
        return None

# Example of how the agent would initiate the planning process:
# 1. Planner generates DAG file on disk.
# 2. Agent triggers the DAG run.

# trigger_airflow_dag("action_graph_plan_dinner_1", {"goal": "achieve_dinner"})
