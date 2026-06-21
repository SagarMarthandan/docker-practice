from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="02_python",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    tags=["zoomcamp"],
    catchup=False,
)
def python_dag():

    @task
    def get_dockerhub_pulls() -> int:
        import requests
        url = "https://hub.docker.com/v2/repositories/kestra/kestra"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        pulls = response.json().get("pull_count", 0)
        print(f"DockerHub pull count: {pulls:,}")
        return pulls  # returned value is stored as XCom automatically

python_dag()