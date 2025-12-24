from cleaner import run_cleaner
from kpi_job import run_kpi_job
from export_csv import export_csv
from logger import logger

def run_pipeline():
    logger.info("Pipeline started")

    try:
        run_cleaner()
        logger.info("Cleaner step finished")

        run_kpi_job()
        logger.info("KPI job step finished")

        export_csv()
        logger.info("CSV export step finished")

        logger.info("Pipeline finished successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()
