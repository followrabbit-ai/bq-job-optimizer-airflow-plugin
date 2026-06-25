from setuptools import setup

setup(
    name="rabbit-bq-optimizer-airflow-plugin",
    version="1.0.0",
    py_modules=["rabbit_bq_optimizer_plugin"],
    package_dir={"": "bq-job-optimizer-airflow-2"},
    install_requires=[
        "rabbit-bq-job-optimizer>=0.1.18",
    ],
    author="Rabbit Team",
    author_email="success@followrabbit.ai",
    description="Airflow 2 plugin for Rabbit BigQuery Job Optimizer",
    long_description=open("bq-job-optimizer-airflow-2/README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/followrabbit-ai/bq-job-optimizer-airflow-plugin",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Framework :: Apache Airflow",
    ],
    python_requires=">=3.8",
    entry_points={
        "airflow.plugins": [
            "rabbit_bq_job_optimizer_plugin = "
            "rabbit_bq_optimizer_plugin:RabbitBQOptimizerPlugin",
        ],
    },
)
