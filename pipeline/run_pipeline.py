from cleaner import run_cleaner
from kpi_job import run_kpi_job
from export_csv import export_csv

def run_pipeline():
    print("Pipeline started.")

    run_cleaner()
    run_kpi_job()
    export_csv()

    print("Pipeline finished successfully.")

if __name__ == "__main__":
    run_pipeline()
