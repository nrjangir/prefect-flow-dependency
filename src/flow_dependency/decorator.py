import asyncio
import logging
import os
from functools import wraps
from time import time, sleep
from typing import Optional, Callable, Union
import pendulum
from prefect import get_client, task
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterStartTime,
    DeploymentFilter,
    DeploymentFilterName
)
from prefect.client.schemas.sorting import FlowRunSort

logger = logging.getLogger('prefect.flow_run_dependency')
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

class WaitDeploymentsError(Exception):
    pass

@task
async def set_current_time_utc():
    """
       Set the CURRENT_TIME_UTC environment variable to the current UTC time.

       This function obtains the current time in UTC using Pendulum, converts it to a string,
       and sets it as the value of the CURRENT_TIME_UTC environment variable.

       Returns:
    """
    try:
        current_time_utc = str(pendulum.now('UTC'))
        os.environ['CURRENT_TIME_UTC'] = current_time_utc
        logger.info(f"CURRENT_TIME_UTC set to: {current_time_utc}")
        return True
    except Exception as e:
        error_message = f"Failed to set CURRENT_TIME_UTC. Error: {e}"
        logger.error(error_message)
        return False


async def get_flow_run_time_utc_from_env(env_var_name='CURRENT_TIME_UTC', format_string='YYYY-MM-DDTHH:mm:ss.SSSSSSZ'):
    """
    Retrieve the UTC start time from the specified environment variable and format it using Pendulum.

    Parameters:
    - env_var_name (str, optional): The name of the environment variable. Default is 'CURRENT_TIME_UTC'.
    - format_string (str, optional): The format string for the time representation. Default is 'YYYY-MM-DDTHH:mm:ss.SSSSSSZ'.

    Returns:
    - pendulum.DateTime: The Pendulum representation of the UTC start time.
    """
    if os.environ.get(env_var_name):
        _timestamp = pendulum.from_format(os.environ.get(env_var_name), format_string).in_tz('UTC')
        logging.info(f"Environment variable `{env_var_name}` found: {_timestamp}")
        return _timestamp
    else:
        _timestamp = pendulum.now('UTC')
        logging.warning(
            f"Environment variable `{env_var_name}` not found or empty. Setting current timestamp as {_timestamp}")
        return _timestamp


async def fetch_latest_runs(client, deploy_names, flow_run_time):
    """
    Fetch the latest flow runs for the specified deployment names.

    Parameters:
    - client (prefect.Client): Prefect client for API interactions.
    - deploy_names (list): List of deployment names to fetch runs for.
    - flow_run_time (pendulum.DateTime): The starting time for flow run filtering.
    - check_last_hours (int, optional): The number of hours to consider for flow run filtering. Default is 1 hour.


    Returns:
    - list: a list of Flow Run model representations of the flow runs
    """
    states = await client.read_flow_runs(
        flow_run_filter = FlowRunFilter(
            start_time = FlowRunFilterStartTime(after_ = flow_run_time)
        ),
        deployment_filter = DeploymentFilter(
            name = DeploymentFilterName(any_ = list(deploy_names))
        ),
        sort = FlowRunSort.START_TIME_DESC,
        limit = len(deploy_names)
    )
    return states


def wait_for_deployments(deployments, flow_run_time_utc_source=None, check_last_hours: int = 0,
                         deployment_timeout: int = 259200, retry_span: int = 60):
    """
    Decorator to wait for specified Prefect flow deployments to complete before triggering the decorated flow function.

    Parameters:
    - deployments (List[str]): List of deployment names to wait for.
    - flow_run_time_utc_source (Union[pendulum.DateTime, Callable], optional): Source for the start time in UTC.
      It can be a UTC value, a synchronous function, or an asynchronous function.
      Default is None, which uses get_flow_run_time_utc_from_env.
    - check_last_hours (int, optional): The number of hours to consider for flow run filtering. Default is 1 hour.
    - deployment_timeout (int, optional): Maximum time (in seconds) to wait for the deployments. Default is 500 seconds.
    - retry_span (int, optional): Time (in seconds) to wait between checking for the latest flow runs. Default is 5 seconds.

    Returns:
    - Callable: Decorated async function.
    """

    async def get_flow_run_time_utc(flow_run_time_source: Optional[
        Union[pendulum.DateTime, Callable[..., Union[pendulum.DateTime, asyncio.Future]]]] = None, *args,
                                    **kwargs) -> pendulum.DateTime:
        """
        Get the flow run time in UTC based on the provided source.

        Parameters:
        - flow_run_time_source (Optional[Union[pendulum.DateTime, Callable[..., Union[pendulum.DateTime, asyncio.Future]]]]):
          Source for flow run time in UTC. It can be a UTC value, a synchronous function, or an asynchronous function.

        Returns:
        - pendulum.DateTime: Flow run time in UTC.

        Raises:
        - TypeError: If the flow_run_time_source is not a UTC value, a synchronous function, or an asynchronous function.
        """
        if flow_run_time_source is None:
            logging.info("flow_run_time_source is `None`, so using current timestamp.")
            return pendulum.now('UTC')
        elif isinstance(flow_run_time_source, pendulum.DateTime):
            logging.info("Using provided UTC value directly.")
            return flow_run_time_source
        elif callable(flow_run_time_source):
            func_name = flow_run_time_source.__name__ if hasattr(flow_run_time_source,
                                                                 "__name__") else "Unknown Function"
            logging.info(f"Using result from callable function: {func_name}")
            result = flow_run_time_source(*args, **kwargs)

            if asyncio.iscoroutine(result):
                logging.info("Asynchronous function result.")
                result = await result

            if isinstance(result, pendulum.DateTime):
                logging.info("Result is of type pendulum.DateTime.")
                return result
            else:
                raise TypeError(f"The result from the callable function {func_name} is not of type pendulum.DateTime. "
                                f"Got {type(result)} instead.")
        else:
            raise TypeError("Invalid flow_run_time_source. It should be a UTC value, a synchronous function, "
                            "or an asynchronous function.")

    def decorator(flow_func):
        @wraps(flow_func)
        async def wrapper(*args, **kwargs):
            """
            Wrapper function that waits for specified deployments to complete before triggering the decorated flow function.
            """
            nonlocal deployments
            try:
                sleep(60)
                if not isinstance(deployments, list):
                    deployments = [deployments]

                completed_deployments = set()
                pending_deployments = set(deployments)
                start_time_utc = await get_flow_run_time_utc(flow_run_time_utc_source)
                client = get_client()
                flow_run_time = start_time_utc.subtract(hours = check_last_hours)

                logger.info(f"Waiting for the deployment(s) to complete: `{deployments}`")
                logger.info(f"Decorator: Start time for deployment tracking from: {flow_run_time}")

                while time() - start_time_utc.timestamp() < deployment_timeout:
                    states = await fetch_latest_runs(client, pending_deployments, flow_run_time)
                    for latest_run in states:
                        _deployment = await client.read_deployment(latest_run.deployment_id)
                        _deployment_name = _deployment.name
                        _state = latest_run.state_name
                        logger.info(
                            f"Latest flow run for `{_deployment_name}` found at {latest_run.start_time} with state {_state}.")

                        if _state == 'Completed' and _deployment_name not in completed_deployments:
                            completed_deployments.add(_deployment_name)
                            pending_deployments.remove(_deployment_name)
                            logger.info(
                                f"Flow run for deployment `{_deployment_name}` completed at {latest_run.start_time}.")

                            if not pending_deployments:
                                logger.info(f"All deployments completed. Triggering {flow_func.__name__} ...")
                                if asyncio.iscoroutinefunction(flow_func):
                                    return asyncio.run(flow_func(*args, **kwargs))
                                else:
                                    return flow_func(*args, **kwargs)

                    logger.info(
                        f"Waiting for the latest flow runs ({round((time() - start_time_utc.timestamp()))}/{deployment_timeout} sec)")
                    logger.info(f"Here is pending deployments count: {len(pending_deployments)}")
                    sleep(retry_span)

                logger.error(f"The maximum deployment timeout exceeded. Marking the flow as 'Failed'.")
                logger.error("Deployments not completed within the specified timeout. Taking appropriate action...")
                return False

            except Exception as e:
                raise WaitDeploymentsError(f"An unexpected error occurred in wait_for_deployments: {e}")

        return wrapper

    return decorator