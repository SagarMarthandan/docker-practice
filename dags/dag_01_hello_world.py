from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="01_hello_world",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # manual trigger only (same as no Kestra trigger)
    tags=["zoomcamp"],
    catchup=False,
)
def hello_world():

    @task
    def greet():
        print("Hello from Airflow! (was: Hello, Kestra!)")
        return "Hello, World!"

    greet()

hello_world()