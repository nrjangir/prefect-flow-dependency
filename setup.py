from setuptools import setup, find_packages

setup(
    name="prefect-flow-dependency",
    version="0.1.0",
    description="A decorator for managing flow-level dependencies in Prefect 2.x",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "prefect>=3.0",
        "pendulum",
        "pytest"
    ],
    python_requires=">=3.8",
)
