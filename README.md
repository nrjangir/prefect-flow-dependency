# 🧭 Prefect Flow Dependency Decorator

A utility to manage **flow-level dependencies** in [Prefect 2.x](https://docs.prefect.io/) using a simple and extensible decorator.

> ✨ Useful when one flow must wait for other flow deployments to finish before executing.

---

## 🚀 Features

- ✅ Waits for one or more Prefect deployments to finish before triggering a flow  
- ✅ Supports both **async** and **sync** flows  
- ✅ Customizable:
  - Timeout duration
  - Retry intervals
  - Filtering on recent runs
- ✅ Built-in support for time-based filtering via environment variable  
- ✅ Easy to plug into existing Prefect 2.x pipelines  

---

## 📦 Installation

```bash
pip install git+https://github.com/<your-username>/prefect-flow-dependency.git
```

## 🧩 Usage Example
```python
from prefect import flow
from flow_dependency import wait_for_deployments

@flow
@wait_for_deployments(deployments=["flow-a", "flow-b"], retry_span=30, deployment_timeout=1800)
async def my_main_flow():
    print("All dependencies completed. Proceeding with main flow...")
```

## 🧪 Example Flow
```python
from prefect import flow, task
from flow_dependency import wait_for_deployments, set_current_time_utc

@task
def extract():
    print("Extracting data...")

@task
def transform():
    print("Transforming data...")

@task
def load():
    print("Loading data...")

@flow
@wait_for_deployments(deployments=["data-ingestion-flow"], retry_span=60, deployment_timeout=3600)
async def etl_pipeline():
    extract()
    transform()
    load()
```

## 🧠 How It Works
The @wait_for_deployments decorator checks Prefect Cloud or Server for the latest flow runs from specified deployments.

It:
- Checks if deployments completed successfully.
- Waits and retries if still running.
- Times out if the max wait time is exceeded.
- Uses pendulum for time handling and prefect.client APIs for deployment/run info.

## ⚙️ Configuration Options
| Parameter                  | Type                            | Description                                                                 |
| -------------------------- | ------------------------------- | --------------------------------------------------------------------------- |
| `deployments`              | `List[str]`                     | List of deployment names to wait for                                        |
| `flow_run_time_utc_source` | `pendulum.DateTime \| Callable` | Optional: time source for filtering flow runs                               |
| `check_last_hours`         | `int`                           | How many hours back to look for flow runs (default: 0)                      |
| `deployment_timeout`       | `int`                           | Max time to wait for dependencies to complete, in seconds (default: 259200) |
| `retry_span`               | `int`                           | Retry interval in seconds (default: 60)                                     |

## 🔧 Environment-Based Time Control
To dynamically control the time range of flow run filtering, you can set the current UTC time into an environment variable using:
```python
from flow_dependency import set_current_time_utc

await set_current_time_utc()
```

Then pass get_flow_run_time_utc_from_env to the decorator:

```python
from flow_dependency import get_flow_run_time_utc_from_env

@flow
@wait_for_deployments(
    deployments=["flow-a"],
    flow_run_time_utc_source=get_flow_run_time_utc_from_env
)
async def my_flow():
    ...
```
## 🧪 Testing (Basic)
```bash
pytest tests/
Minimal tests can be added using mock clients or Prefect’s test utilities.
```

## 📁 Project Structure
```bash
prefect-flow-dependency/
├── src/
│   └── flow_dependency/
│       ├── __init__.py
│       └── decorator.py
├── examples/
│   └── sample_usage.py
├── tests/
│   └── test_decorator.py
├── README.md
├── setup.py
├── LICENSE
└── .gitignore
```

## 🙌 Contributing
Contributions, issues, and suggestions are welcome!

### To contribute:

- Fork the repo
- Create your feature branch (git checkout -b feature/foo)
- Commit your changes (git commit -am 'Add some foo')
- Push to the branch (git push origin feature/foo)
- Create a new Pull Request

## 📜 License
MIT License © 2025 Naren K