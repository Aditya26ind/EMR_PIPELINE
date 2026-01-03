import sys

pipeline = sys.argv[1]

if pipeline=="sales":
    from pipelines.sales_etl.job import run
elif pipeline == "user_activity":
    from pipelines.user_activity_etl.job import run
elif pipeline == "inventory":
    from pipelines.inventory_etl.job import run

run()