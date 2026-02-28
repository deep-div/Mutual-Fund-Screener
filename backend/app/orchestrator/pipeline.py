from app.ingestion.mfapi_data import MFAPIFetcher
from app.analytics.metrics import NavMetrics
import asyncio
import nest_asyncio
nest_asyncio.apply()
from app.shared.logger import logger
import json

def main():
    """Run MF ingestion and analytics and return meta + metrics."""
    try:
        logger.info("Starting mutual fund pipeline execution")

        fetcher = MFAPIFetcher()

        days = 7
        schemes_list = fetcher.fetch_recent_active_schemes(days)
        logger.info(f"Fetched {len(schemes_list)} schemes")

        raw_data = asyncio.run(fetcher.fetch_schemes_from_list(schemes_list[:2]))

        final_response = []

        for scheme in raw_data:
            meta = scheme.get("meta", {})
            nav_data = scheme.get("data", [])

            metrics = NavMetrics(nav_data)
            metrics_output = metrics.get_all_metrics()

            final_response.append({
                "meta": meta,
                "metrics": metrics_output
            })

        logger.info("Pipeline execution completed successfully")
        return final_response

    except Exception as e:
        logger.exception(f"Fatal error in main execution: {e}")
        return []


# if __name__ == "__main__":
#     response = main()
#     print(json.dumps(response, indent=2))