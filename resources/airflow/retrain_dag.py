from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configuración por defecto de las tareas
default_args = {
    "owner": "marta",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# El comando de reentrenamiento: el mismo spark-submit que lanzas a mano
SPARK_SUBMIT = (
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "/opt/airflow/project/resources/train_spark_mllib_model.py "
    "/opt/airflow/project"
)

with DAG(
    dag_id="flight_delay_retraining",
    default_args=default_args,
    start_date=datetime(2026, 7, 9),
    schedule_interval=None,
    catchup=False,
    tags=["ibdn", "spark", "mllib"],
    dagrun_timeout=None,
) as dag:

    retrain = BashOperator(
        task_id="retrain_model",
        bash_command=SPARK_SUBMIT,
    )