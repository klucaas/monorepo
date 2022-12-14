"""Example DAG demonstrating the usage of the XComArgs."""
import logging

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)


@task
def generate_value():
    """Empty function"""
    return "Bring me a shrubbery!"


@task
def print_value(value, ts=None):
    """Empty function"""
    log.info("The knights of Ni say: %s (at %s)", value, ts)


with DAG(
    dag_id='example_xcom_args',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=['example'],
) as dag:
    print_value(generate_value())

with DAG(
    "example_xcom_args_with_operators",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=['example'],
) as dag2:
    bash_op1 = BashOperator(task_id="c", bash_command="echo c")
    bash_op2 = BashOperator(task_id="d", bash_command="echo c")
    xcom_args_a = print_value("first!")
    xcom_args_b = print_value("second!")

    bash_op1 >> xcom_args_a >> xcom_args_b >> bash_op2