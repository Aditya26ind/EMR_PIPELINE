import sys

if len(sys.argv) < 2:
    raise Exception("Pipeline name required: sales | user_activity")

pipeline = sys.argv[1]

if pipeline == "sales":
    from pipelines.sales_etl.job import run
elif pipeline == "user_activity":
    from pipelines.user_activity_etl.job import run
else:
    raise Exception(f"Unknown pipeline: {pipeline}")

run()
