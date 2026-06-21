-- Creates the zoomcamp database for taxi data alongside the airflow metadata db
CREATE DATABASE zoomcamp;
GRANT ALL PRIVILEGES ON DATABASE zoomcamp TO airflow;