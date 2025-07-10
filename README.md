---

# AirflowTest DAGs Repository

This repository contains **Airflow DAGs** for file ingestion workflows using **Git Sync** for deployment. The DAGs focus on ingesting files into **S3-compatible storage** (e.g., MinIO) and **DorisDB**.

---

## Features

* **Git Sync integration**: Automatically sync DAG files into Airflow pods.
* **File ingestion pipelines**:

  * Upload files from local or network locations to S3 (MinIO).
  * Load data from files into DorisDB.
* Built using **PySpark** and other Python operators.


---

## Prerequisites

* Airflow environment with **Git Sync sidecar** configured to sync this repo's `dags/` folder.
* Airflow Docker images include required Python packages (e.g., `pyspark`).
* Access to an S3-compatible object storage (like MinIO) with credentials configured.
* DorisDB connection configured in Airflow (via connection or environment variables).
* Spark Connect server accessible by Airflow workers.

---

