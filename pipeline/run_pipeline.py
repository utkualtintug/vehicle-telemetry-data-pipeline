from ingest_csv import run_ingest
from kpi_job import run_kpi_job
from export_csv import export_csv
from logger import logger

def run_pipeline():
    logger.info("Pipeline started")

    try:
        run_ingest()

        run_kpi_job()

        export_csv()
        
        logger.info("Pipeline finished successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()
