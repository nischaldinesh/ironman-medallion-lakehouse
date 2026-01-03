from datetime import datetime, timedelta
import io

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


S3_BUCKET = ""
S3_PREFIX = "inbound"


RDS_CONN_ID = ""
AWS_CONN_ID = "aws_default"
DATABRICKS_CONN_ID = "databricks_default"


DATABRICKS_JOB_ID = ""


TABLES_CONFIG = [
    {"year": 2023, "gender": "M", "table": "2023_men", "filename": "2023_men.csv"},
    {"year": 2023, "gender": "F", "table": "2023_women", "filename": "2023_women.csv"},
    {"year": 2024, "gender": "M", "table": "2024_men", "filename": "2024_men.csv"},
    {"year": 2024, "gender": "F", "table": "2024_women", "filename": "2024_women.csv"},
    {"year": 2025, "gender": "M", "table": "2025_men", "filename": "2025_men.csv"},
    {"year": 2025, "gender": "F", "table": "2025_women", "filename": "2025_women.csv"},
]

LATEST_YEAR = max(c["year"] for c in TABLES_CONFIG)




def _get_process_year(context) -> int:
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    year = conf.get("process_year")
    if year is None or str(year).strip() == "":
        raise ValueError("process_year is required. Trigger the DAG with e.g. {'process_year': 2024}")
    return int(year)

def extract_and_upload_to_s3(table_name: str, filename: str, year: int, gender: str, **context):
    selected_year = _get_process_year(context)

    if int(year) != int(selected_year):
        print("=" * 60)
        print(f"SKIP: {table_name} (task year={year}) because process_year={selected_year}")
        print("=" * 60)
        return {"status": "skipped", "table": table_name, "year": year, "gender": gender}

    print("=" * 60)
    print(f"TASK: Extract {table_name} and Upload to S3")
    print("=" * 60)

   
    print(f"\n[1/3] Connecting to RDS...")
    pg_hook = PostgresHook(postgres_conn_id=RDS_CONN_ID)

    sql = f'SELECT * FROM "{table_name}"'
    print(f"Executing: {sql}")

    df = pg_hook.get_pandas_df(sql)
    row_count = len(df)
    print(f"Extracted {row_count:,} rows from {table_name}")

    if row_count == 0:
        raise ValueError(f"No data found in table {table_name}")

    print(f"\n[2/3] Converting to CSV...")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()
    csv_size = len(csv_content)
    print(f"CSV size: {csv_size:,} bytes")

    print(f"\n[3/3] Uploading to S3...")
    s3_key = f"{S3_PREFIX}/year={selected_year}/{filename}"

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_hook.load_string(
        string_data=csv_content,
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=False, 
    )

    s3_path = f"s3://{S3_BUCKET}/{s3_key}"
    print(f"\nSuccessfully uploaded to {s3_path}")
    print(f"   Rows: {row_count:,}")
    print(f"   Size: {csv_size:,} bytes")

    return {
        "table": table_name,
        "s3_path": s3_path,
        "rows": row_count,
        "size_bytes": csv_size,
        "year": year,
        "gender": gender,
        "status": "success",
    }

def validate_s3_files(**context):
    print("=" * 60)
    print("TASK: Validate S3 Files")
    print("=" * 60)

    selected_year = _get_process_year(context)

    expected_files = [c["filename"] for c in TABLES_CONFIG if c["year"] == selected_year]
    if not expected_files:
        raise ValueError(f"No TABLES_CONFIG entries found for process_year={selected_year}")

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    missing_files = []
    total_size = 0

    print(f"\nChecking files in S3 for year={selected_year}:")
    for filename in expected_files:
        s3_key = f"{S3_PREFIX}/year={selected_year}/{filename}"

        try:
            exists = s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET)
            if exists:
                s3_client = s3_hook.get_conn()
                response = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
                file_size = response["ContentLength"]
                total_size += file_size
                print(f"{filename} ({file_size:,} bytes)")
            else:
                print(f"{filename} - NOT FOUND")
                missing_files.append(filename)
        except Exception as e:
            print(f"{filename} - ERROR: {e}")
            missing_files.append(filename)

    if missing_files:
        raise ValueError(
            f"Missing files in S3 for year={selected_year}: {missing_files}. "
            f"Expected under s3://{S3_BUCKET}/{S3_PREFIX}/year={selected_year}/"
        )

    print(f"\nAll {len(expected_files)} files validated successfully!")
    print(f"Total size: {total_size:,} bytes")

    return {
        "status": "success",
        "process_year": selected_year,
        "files_validated": len(expected_files),
        "total_size_bytes": total_size,
    }

def notify_success(**context):
    print("=" * 60)
    print("PIPELINE SUCCESS")
    print("=" * 60)

    execution_date = context.get("ds")
    dag_run = context.get("dag_run")
    conf = dag_run.conf or {}
    selected_year = conf.get("process_year")

    print(f"DAG: {dag_run.dag_id}")
    print(f"Execution Date: {execution_date}")
    print(f"Run ID: {dag_run.run_id}")
    print(f"Processed Year: {selected_year}")
    print("\nAll tasks completed successfully!")

    return {"status": "success", "process_year": selected_year}

def notify_failure(context):
    """
    Failure callback.
    """
    print("=" * 60)
    print("PIPELINE FAILED")
    print("=" * 60)

    task_instance = context.get("task_instance")
    exception = context.get("exception")

    print(f"Failed Task: {task_instance.task_id}")
    print(f"Error: {exception}")


with DAG(
    dag_id="ironman_dag",
    description="Ironman Pipeline: RDS to S3 to Databricks (yearly incremental)",
    schedule=None, 
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ironman", "production", "databricks"],
    doc_md=__doc__,
    on_failure_callback=notify_failure,
) as dag:


    with TaskGroup(group_id="extract_and_upload") as extract_upload_group:
        for config in TABLES_CONFIG:
            PythonOperator(
                task_id=f"extract_upload_{config['table']}",
                python_callable=extract_and_upload_to_s3,
                op_kwargs={
                    "table_name": config["table"],
                    "filename": config["filename"],
                    "year": config["year"],
                    "gender": config["gender"],
                },
            )

    validate = PythonOperator(
        task_id="validate_s3_files",
        python_callable=validate_s3_files,
    )

    trigger_databricks = DatabricksRunNowOperator(
        task_id="trigger_databricks_pipeline",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,

      
        idempotency_token="{{ dag_run.run_id }}",

        notebook_params={
            "run_mode": "{{ dag_run.conf.get('run_mode', 'incremental') }}",
            "process_year": "{{ dag_run.conf.get('process_year', '" + str(LATEST_YEAR) + "') }}",
            "triggered_by": "airflow",
            "execution_date": "{{ ds }}",
        },

        wait_for_termination=True,
        polling_period_seconds=30,
    )
    success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )


    extract_upload_group >> validate >> trigger_databricks >> success
