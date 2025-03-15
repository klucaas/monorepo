import yaml
from airflow.models import Connection
from airflow.settings import Session
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule=None, start_date=datetime(2025, 3, 1), catchup=False, tags=["setup"])
def load_airflow_connections():
    @task
    def load_connections():
        with open("/opt/airflow/connections.yaml", "r") as file:
            data = yaml.safe_load(file)

        session = Session()
        for conn in data.get("connections", []):
            existing_conn = session.query(Connection).filter(Connection.conn_id == conn["conn_id"]).first()
            if existing_conn:
                print(f"Connection {conn['conn_id']} already exists. Skipping...")
            else:
                new_conn = Connection(
                    conn_id=conn["conn_id"],
                    conn_type=conn["conn_type"],
                    host=conn.get("host"),
                    schema=conn.get("schema"),
                    login=conn.get("login"),
                    password=conn.get("password"),
                    port=conn.get("port"),
                    extra=conn.get("extra"),
                )
                session.add(new_conn)
                print(f"Added new connection: {conn['conn_id']}")

        session.commit()
        session.close()

    load_connections()

load_airflow_connections()