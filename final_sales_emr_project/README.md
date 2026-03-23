# Sales -> S3 ETL (EMR + Airflow) — Final Minimal Project (uses Books.toscrape demo site)

This project is a runnable example that demonstrates:
- Scraping product data from a *real scraping sandbox* website (https://books.toscrape.com)
- Running a PySpark job on EMR triggered by Airflow
- Writing partitioned Parquet output to S3 for analytics

**Key files**
- `dags/sales_to_s3_emr_dag.py` — Airflow DAG that uploads the PySpark job to S3, creates EMR cluster, runs the job, waits for completion, and terminates the cluster.
- `jobs/fetch_sales_spark.py` — PySpark job that crawls Books.toscrape and writes Parquet to S3.
- `scripts/upload_job_to_s3.py` — helper to upload the job file to S3.
- `requirements.txt`, `.env.example`, `.gitignore`

**Important notes before running**
1. This project scrapes **books.toscrape.com** — a site intentionally created as a sandbox for practicing web scraping. It is safe to scrape for demos and learning. See https://books.toscrape.com. (This is NOT a real retailer; it's a sandbox.)

2. Configure AWS credentials for the Airflow worker (or use an IAM role) with permissions for S3 and EMR. The DAG expects an Airflow Variable named `sales_bucket` containing your S3 bucket name. The DAG also reads optional variables for EMR roles and log URI (`job_flow_role`, `service_role`, `emr_log_uri`, `ec2_key_name`).

3. The DAG's EMR instance types (m5.xlarge) and counts are examples — change them to suit your account/quota/costs.

4. On EMR, make sure `requests` and `beautifulsoup4` are available to the PySpark job (you can ship them as a bootstrap action, use a bootstrap script to pip install, or create a custom EMR AMI / bootstrap). For quick tests, run the PySpark job locally first.

**How to test locally**
- Install dependencies: `pip install pyspark requests beautifulsoup4`
- Run locally:
  ```bash
  spark-submit jobs/fetch_sales_spark.py --start_url "https://books.toscrape.com/" --output_s3 "/tmp/books_output/"
  ```
  (When running locally, `--output_s3` can be a local path like `/tmp/...`)

**Where output goes**
- The job writes Parquet files partitioned by `date` into the `--output_s3` path you provide (e.g., `s3://your-bucket/output/books/`)

**References & rationale**
- books.toscrape.com is a scraping sandbox suitable for demos and testing scraping code.
- Airflow EMR operators (EmrCreateJobFlowOperator, EmrAddStepsOperator) provide a straightforward create-submit-wait-terminate flow.
- Parquet partitioned by date is analytics-friendly.

Generated on: 2026-01-26T10:40:10.483805 UTC
