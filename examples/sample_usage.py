from prefect import flow, task, get_run_logger
import time

from src.flow_dependency.decorator import wait_for_deployments, get_flow_run_time_utc_from_env


@task
def subflow_1_task():
    logger = get_run_logger()
    logger.info("Subflow 1 task started. Sleeping for 60 seconds.")
    time.sleep(60)
    logger.info("Subflow 1 task completed.")

@task
def subflow_2_task():
    logger = get_run_logger()
    logger.info("Subflow 2 task started. Sleeping for 90 seconds.")
    time.sleep(90)
    logger.info("Subflow 2 task completed.")

@task
def subflow_3_task():
    logger = get_run_logger()
    logger.info("Subflow 3 task executed.")

@flow(name="subflow-1")
def subflow_1():
    subflow_1_task()

@flow(name="subflow-2")
def subflow_2():
    subflow_2_task()

@flow(name="subflow-3")
def subflow_3():
    subflow_3_task()

@flow(name="main-flow")
@wait_for_deployments(
    deployments=["subflow-1", "subflow-2"],
    flow_run_time_utc_source=get_flow_run_time_utc_from_env,
    check_last_hours=1,
    deployment_timeout=600,   # 10 minutes timeout
    retry_span=15             # check every 15 seconds
)
def main_flow():
    logger = get_run_logger()
    logger.info("Main flow started after waiting for subflows to complete.")
    subflow_3()
