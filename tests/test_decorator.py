import logging
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import pendulum
import pytest
from prefect.client.schemas.objects import FlowRun, State
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterStartTime,
    DeploymentFilter,
    DeploymentFilterName
)
from prefect.client.schemas.sorting import FlowRunSort

from src.flow_dependency.decorator import wait_for_deployments, WaitDeploymentsError, fetch_latest_runs, \
    get_flow_run_time_utc_from_env, set_current_time_utc

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] - %(message)s')

class TestFlowRunDependency(unittest.TestCase):
    @pytest.mark.asyncio
    async def test_set_current_time_utc_success(self):
        with patch('os.environ', new={}) as mock_env:
            with patch('pendulum.now', return_value=pendulum.parse('2025-08-07T12:00:00Z')):
                result = await set_current_time_utc()
                self.assertTrue(result)
                self.assertEqual(mock_env['CURRENT_TIME_UTC'], '2025-08-07T12:00:00+00:00')

    @pytest.mark.asyncio
    async def test_set_current_time_utc_failure(self):
        with patch('os.environ', new={}) as mock_env:
            with patch('pendulum.now', side_effect=Exception("Pendulum error")):
                with patch('logging.error') as mock_log_error:
                    result = await set_current_time_utc()
                    self.assertFalse(result)
                    mock_log_error.assert_called_once()
                    self.assertTrue("Failed to set CURRENT_TIME_UTC" in mock_log_error.call_args[0][0])

    @pytest.mark.asyncio
    async def test_get_flow_run_time_utc_from_env_with_valid_env(self):
        with patch('os.environ', new={'CURRENT_TIME_UTC': '2025-08-07T12:00:00.000000Z'}):
            with patch('logging.info') as mock_log_info:
                result = await get_flow_run_time_utc_from_env()
                self.assertIsInstance(result, pendulum.DateTime)
                self.assertEqual(result.to_iso8601_string(), '2025-08-07T12:00:00+00:00')
                mock_log_info.assert_called_once()
                self.assertTrue("Environment variable `CURRENT_TIME_UTC` found" in mock_log_info.call_args[0][0])

    @pytest.mark.asyncio
    async def test_get_flow_run_time_utc_from_env_without_env(self):
        with patch('os.environ', new={}):
            with patch('pendulum.now', return_value=pendulum.parse('2025-08-07T12:00:00Z')):
                with patch('logging.warning') as mock_log_warning:
                    result = await get_flow_run_time_utc_from_env()
                    self.assertIsInstance(result, pendulum.DateTime)
                    self.assertEqual(result.to_iso8601_string(), '2025-08-07T12:00:00+00:00')
                    mock_log_warning.assert_called_once()
                    self.assertTrue("Environment variable `CURRENT_TIME_UTC` not found" in mock_log_warning.call_args[0][0])

    @pytest.mark.asyncio
    async def test_fetch_latest_runs(self):
        mock_client = AsyncMock()
        mock_client.read_flow_runs = AsyncMock(return_value=[
            FlowRun(id="1", deployment_id="dep1", state_name="Completed", start_time=pendulum.now('UTC'))
        ])
        deploy_names = ["dep1"]
        flow_run_time = pendulum.parse('2025-08-07T12:00:00Z')

        result = await fetch_latest_runs(mock_client, deploy_names, flow_run_time)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].deployment_id, "dep1")
        mock_client.read_flow_runs.assert_called_once_with(
            flow_run_filter=FlowRunFilter(start_time=FlowRunFilterStartTime(after_=flow_run_time)),
            deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=deploy_names)),
            sort=FlowRunSort.START_TIME_DESC,
            limit=len(deploy_names)
        )

    @pytest.mark.asyncio
    async def test_wait_for_deployments_completed(self):
        @wait_for_deployments(deployments=["dep1"], deployment_timeout=10, retry_span=1)
        async def dummy_flow():
            return "Flow executed"

        mock_client = AsyncMock()
        mock_client.read_flow_runs = AsyncMock(return_value=[
            FlowRun(id="1", deployment_id="dep1", state_name="Completed", start_time=pendulum.now('UTC'))
        ])
        mock_client.read_deployment = AsyncMock(return_value=MagicMock(name="dep1"))

        with patch('prefect.get_client', return_value=mock_client):
            with patch('pendulum.now', return_value=pendulum.parse('2025-08-07T12:00:00Z')):
                result = await dummy_flow()
                self.assertEqual(result, "Flow executed")

    @pytest.mark.asyncio
    async def test_wait_for_deployments_timeout(self):
        @wait_for_deployments(deployments=["dep1"], deployment_timeout=1, retry_span=1)
        async def dummy_flow():
            return "Flow executed"

        mock_client = AsyncMock()
        mock_client.read_flow_runs = AsyncMock(return_value=[
            FlowRun(id="1", deployment_id="dep1", state_name="Running", start_time=pendulum.now('UTC'))
        ])
        mock_client.read_deployment = AsyncMock(return_value=MagicMock(name="dep1"))

        with patch('prefect.get_client', return_value=mock_client):
            with patch('pendulum.now', return_value=pendulum.parse('2025-08-07T12:00:00Z')):
                with patch('time.time', side_effect=[0, 2]):  # Simulate timeout
                    result = await dummy_flow()
                    self.assertFalse(result)

    @pytest.mark.asyncio
    async def test_wait_for_deployments_error(self):
        @wait_for_deployments(deployments=["dep1"], deployment_timeout=10, retry_span=1)
        async def dummy_flow():
            return "Flow executed"

        mock_client = AsyncMock()
        mock_client.read_flow_runs = AsyncMock(side_effect=Exception("API error"))

        with patch('prefect.get_client', return_value=mock_client):
            with patch('pendulum.now', return_value=pendulum.parse('2025-08-07T12:00:00Z')):
                with self.assertRaises(WaitDeploymentsError):
                    await dummy_flow()

    @pytest.mark.asyncio
    async def test_get_flow_run_time_utc_invalid_source(self):
        @wait_for_deployments(deployments=["dep1"], flow_run_time_utc_source="invalid")
        async def dummy_flow():
            return "Flow executed"

        with self.assertRaises(TypeError):
            await dummy_flow()

    @pytest.mark.asyncio
    async def test_get_flow_run_time_utc_callable_invalid_result(self):
        def invalid_callable():
            return "not a datetime"

        @wait_for_deployments(deployments=["dep1"], flow_run_time_utc_source=invalid_callable)
        async def dummy_flow():
            return "Flow executed"

        with self.assertRaises(TypeError):
            await dummy_flow()

if __name__ == '__main__':
    unittest.main()
