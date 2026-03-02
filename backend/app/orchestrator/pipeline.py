from app.ingestion.mfapi_data import run_ingestion
from app.analytics.metrics import run_analytics
from app.db.write import run_store_in_db
from app.shared.logger import logger

def run_pipeline():
    """Execute full ingestion, analytics, and storage pipeline"""
    try:
        logger.info("Starting pipeline execution")
        raw_data = run_ingestion()
        metrics = run_analytics(raw_data)
        run_store_in_db(metrics)
        logger.info("Pipeline execution completed successfully")
    except Exception as e:
        logger.error(f"Fatal error in pipeline execution: {e}")
        raise


# if __name__ == "__main__":
#     run_pipeline()