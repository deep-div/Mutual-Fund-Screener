from sqlalchemy.dialects.postgresql import insert
from app.db.schema import SchemeMetaORM, SchemeAnalyticsORM
from app.db.session import get_session
from sqlalchemy.sql import func
from app.shared.logger import logger

def safe_get(d, *keys):
    """Safely fetch nested dictionary values without KeyError"""
    for key in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(key)
        if d is None:
            return None
    return d

def bulk_upsert_schema(session, data: list[dict]):
    """Bulk insert or update screener data"""
    rows = []
    for item in data:
        if "meta" not in item or "metrics" not in item:
            continue

        meta = item["meta"]
        metrics = item["metrics"]

        row = {
            **meta,

            # Absolute returns
            "abs_1w": safe_get(metrics, "returns", "absolute_returns_percent", "one_week"),
            "abs_1m": safe_get(metrics, "returns", "absolute_returns_percent", "one_month"),
            "abs_3m": safe_get(metrics, "returns", "absolute_returns_percent", "three_month"),
            "abs_6m": safe_get(metrics, "returns", "absolute_returns_percent", "six_month"),

            # CAGR
            "cagr_1y": safe_get(metrics, "returns", "cagr_percent", "one_year"),
            "cagr_2y": safe_get(metrics, "returns", "cagr_percent", "two_year"),
            "cagr_3y": safe_get(metrics, "returns", "cagr_percent", "three_year"),
            "cagr_4y": safe_get(metrics, "returns", "cagr_percent", "four_year"),
            "cagr_5y": safe_get(metrics, "returns", "cagr_percent", "five_year"),
            "cagr_7y": safe_get(metrics, "returns", "cagr_percent", "seven_year"),
            "cagr_10y": safe_get(metrics, "returns", "cagr_percent", "ten_year"),

            # SIP XIRR
            "sip_xirr_1y": safe_get(metrics, "returns", "sip_returns", "one_year", "xirr_percent"),
            "sip_xirr_2y": safe_get(metrics, "returns", "sip_returns", "two_year", "xirr_percent"),
            "sip_xirr_3y": safe_get(metrics, "returns", "sip_returns", "three_year", "xirr_percent"),
            "sip_xirr_4y": safe_get(metrics, "returns", "sip_returns", "four_year", "xirr_percent"),
            "sip_xirr_5y": safe_get(metrics, "returns", "sip_returns", "five_year", "xirr_percent"),
            "sip_xirr_7y": safe_get(metrics, "returns", "sip_returns", "seven_year", "xirr_percent"),
            "sip_xirr_10y": safe_get(metrics, "returns", "sip_returns", "ten_year", "xirr_percent"),

            # Rolling avg
            "rolling_avg_1y": safe_get(metrics, "returns", "rolling_cagr_percent", "1_year", "summary", "average"),
            "rolling_avg_2y": safe_get(metrics, "returns", "rolling_cagr_percent", "2_year", "summary", "average"),
            "rolling_avg_3y": safe_get(metrics, "returns", "rolling_cagr_percent", "3_year", "summary", "average"),
            "rolling_avg_4y": safe_get(metrics, "returns", "rolling_cagr_percent", "4_year", "summary", "average"),
            "rolling_avg_5y": safe_get(metrics, "returns", "rolling_cagr_percent", "5_year", "summary", "average"),
            "rolling_avg_7y": safe_get(metrics, "returns", "rolling_cagr_percent", "7_year", "summary", "average"),
            "rolling_avg_10y": safe_get(metrics, "returns", "rolling_cagr_percent", "10_year", "summary", "average"),

            # Risk metrics
            "volatility_max": safe_get(metrics, "risk_metrics", "volatility_annualized_percent", "max"),
            "downside_deviation_max": safe_get(metrics, "risk_metrics", "downside_deviation_percent", "max"),
            "skewness_max": safe_get(metrics, "risk_metrics", "skewness", "max"),
            "kurtosis_max": safe_get(metrics, "risk_metrics", "kurtosis", "max"),

            # Risk adjusted
            "sharpe_max": safe_get(metrics, "risk_adjusted_returns", "sharpe_ratio", "max"),
            "sortino_max": safe_get(metrics, "risk_adjusted_returns", "sortino_ratio", "max"),
            "calmar_max": safe_get(metrics, "risk_adjusted_returns", "calmar_ratio", "max"),
            "pain_index_max": safe_get(metrics, "risk_adjusted_returns", "pain_index", "max"),
            "ulcer_index_max": safe_get(metrics, "risk_adjusted_returns", "ulcer_index", "max"),

            # Drawdown
            "current_drawdown_percent": safe_get(metrics, "drawdown", "current_drawdown", "max_drawdown_percent"),
            "mdd_one_year_pct": safe_get(metrics, "drawdown", "mdd_duration_details", "one_year", "max_drawdown_percent"),
            "mdd_two_year_pct": safe_get(metrics, "drawdown", "mdd_duration_details", "two_year", "max_drawdown_percent"),
            "mdd_three_year_pct": safe_get(metrics, "drawdown", "mdd_duration_details", "three_year", "max_drawdown_percent"),
            "mdd_four_year_pct": safe_get(metrics, "drawdown", "mdd_duration_details", "four_year", "max_drawdown_percent"),
            "mdd_five_year_pct": safe_get(metrics, "drawdown", "mdd_duration_details", "five_year", "max_drawdown_percent"),
            "mdd_seven_year_pct": safe_get(metrics, "drawdown", "mdd_duration_details", "seven_year", "max_drawdown_percent"),
            "mdd_ten_year_pct": safe_get(metrics, "drawdown", "mdd_duration_details", "ten_year", "max_drawdown_percent"),
            "mdd_max_drawdown_percent": safe_get(metrics, "drawdown", "mdd_duration_details", "max", "max_drawdown_percent"),
        }

        rows.append(row)

    if not rows:
        return

    stmt = insert(SchemeMetaORM).values(rows)

    update_columns = {
        c.name: getattr(stmt.excluded, c.name)
        for c in SchemeMetaORM.__table__.columns
        if c.name not in ["id", "created_at"]
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=["scheme_code"],
        set_=update_columns
    )

    session.execute(stmt)


"""Bulk insert or update complete mutual fund analytics JSON"""
def bulk_upsert_analytics(session, data: list[dict]):
    rows = [
        {
            "scheme_code": item["meta"]["scheme_code"],
            "full_data": item
        }
        for item in data
        if "meta" in item and "scheme_code" in item["meta"]
    ]

    if not rows:
        return

    stmt = insert(SchemeAnalyticsORM).values(rows)

    stmt = stmt.on_conflict_do_update(
        index_elements=["scheme_code"],
        set_={
            "updated_at": func.now(),
            "full_data": stmt.excluded.full_data
        }
    )

    session.execute(stmt)

"""Runs full mutual fund pipeline in batches"""
def run_store_in_db(data: list[dict], batch_size: int = 500):
    session = get_session()
    total_records = len(data)
    logger.info(f"Starting DB pipeline | Total Records: {total_records} | Batch Size: {batch_size}")

    try:
        for i in range(0, total_records, batch_size):
            chunk = data[i:i + batch_size]
            logger.info(f"Processing batch {(i // batch_size) + 1} | Records: {len(chunk)}")

            bulk_upsert_schema(session, chunk)
            bulk_upsert_analytics(session, chunk)

            session.commit()
            logger.info(f"Batch {(i // batch_size) + 1} committed successfully")

        logger.info("DB pipeline completed successfully")

    except Exception as e:
        session.rollback()
        logger.error(f"DB pipeline failed | Error: {str(e)}", exc_info=True)
        raise

    finally:
        session.close()
        logger.info("DB session closed")
