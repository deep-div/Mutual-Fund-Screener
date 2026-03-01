from sqlalchemy.dialects.postgresql import insert
from app.db.schema import SchemeMetaORM, SchemeAnalyticsORM, BatchORM
from app.db.session import get_session
from sqlalchemy.sql import func


"""Bulk insert or update screener data"""
def bulk_upsert_schema(data: list[dict], batch_id: int):
    session = get_session()
    try:
        rows = []
        for item in data:
            if "meta" not in item or "metrics" not in item:
                continue

            meta = item["meta"]
            metrics = item["metrics"]

            row = {
                **meta,
                "batch_id": batch_id,

                # Absolute returns
                "abs_1w": metrics["returns"]["absolute_returns_percent"].get("one_week"),
                "abs_1m": metrics["returns"]["absolute_returns_percent"].get("one_month"),
                "abs_3m": metrics["returns"]["absolute_returns_percent"].get("three_month"),
                "abs_6m": metrics["returns"]["absolute_returns_percent"].get("six_month"),

                # CAGR
                "cagr_1y": metrics["returns"]["cagr_percent"].get("one_year"),
                "cagr_2y": metrics["returns"]["cagr_percent"].get("two_year"),
                "cagr_3y": metrics["returns"]["cagr_percent"].get("three_year"),
                "cagr_4y": metrics["returns"]["cagr_percent"].get("four_year"),
                "cagr_5y": metrics["returns"]["cagr_percent"].get("five_year"),
                "cagr_7y": metrics["returns"]["cagr_percent"].get("seven_year"),
                "cagr_10y": metrics["returns"]["cagr_percent"].get("ten_year"),

                # SIP XIRR
                "sip_xirr_1y": metrics["returns"]["sip_returns"]["one_year"].get("xirr_percent"),
                "sip_xirr_2y": metrics["returns"]["sip_returns"]["two_year"].get("xirr_percent"),
                "sip_xirr_3y": metrics["returns"]["sip_returns"]["three_year"].get("xirr_percent"),
                "sip_xirr_4y": metrics["returns"]["sip_returns"]["four_year"].get("xirr_percent"),
                "sip_xirr_5y": metrics["returns"]["sip_returns"]["five_year"].get("xirr_percent"),
                "sip_xirr_7y": metrics["returns"]["sip_returns"]["seven_year"].get("xirr_percent"),
                "sip_xirr_10y": metrics["returns"]["sip_returns"]["ten_year"].get("xirr_percent"),

                # Rolling avg
                "rolling_avg_1y": metrics["returns"]["rolling_cagr_percent"]["1_year"]["summary"].get("average"),
                "rolling_avg_2y": metrics["returns"]["rolling_cagr_percent"]["2_year"]["summary"].get("average"),
                "rolling_avg_3y": metrics["returns"]["rolling_cagr_percent"]["3_year"]["summary"].get("average"),
                "rolling_avg_4y": metrics["returns"]["rolling_cagr_percent"]["4_year"]["summary"].get("average"),
                "rolling_avg_5y": metrics["returns"]["rolling_cagr_percent"]["5_year"]["summary"].get("average"),
                "rolling_avg_7y": metrics["returns"]["rolling_cagr_percent"]["7_year"]["summary"].get("average"),
                "rolling_avg_10y": metrics["returns"]["rolling_cagr_percent"]["10_year"]["summary"].get("average"),

                # Risk metrics
                "volatility_max": metrics["risk_metrics"]["volatility_annualized_percent"].get("max"),
                "downside_deviation_max": metrics["risk_metrics"]["downside_deviation_percent"].get("max"),
                "skewness_max": metrics["risk_metrics"]["skewness"].get("max"),
                "kurtosis_max": metrics["risk_metrics"]["kurtosis"].get("max"),

                # Risk adjusted
                "sharpe_max": metrics["risk_adjusted_returns"]["sharpe_ratio"].get("max"),
                "sortino_max": metrics["risk_adjusted_returns"]["sortino_ratio"].get("max"),
                "calmar_max": metrics["risk_adjusted_returns"]["calmar_ratio"].get("max"),
                "pain_index_max": metrics["risk_adjusted_returns"]["pain_index"].get("max"),
                "ulcer_index_max": metrics["risk_adjusted_returns"]["ulcer_index"].get("max"),

                # Drawdown
                "current_drawdown_percent": metrics["drawdown"]["current_drawdown"].get("max_drawdown_percent"),
                "mdd_one_year_pct": metrics["drawdown"]["mdd_duration_details"]["one_year"].get("max_drawdown_percent"),
                "mdd_two_year_pct": metrics["drawdown"]["mdd_duration_details"]["two_year"].get("max_drawdown_percent"),
                "mdd_three_year_pct": metrics["drawdown"]["mdd_duration_details"]["three_year"].get("max_drawdown_percent"),
                "mdd_four_year_pct": metrics["drawdown"]["mdd_duration_details"]["four_year"].get("max_drawdown_percent"),
                "mdd_five_year_pct": metrics["drawdown"]["mdd_duration_details"]["five_year"].get("max_drawdown_percent"),
                "mdd_seven_year_pct": metrics["drawdown"]["mdd_duration_details"]["seven_year"].get("max_drawdown_percent"),
                "mdd_ten_year_pct": metrics["drawdown"]["mdd_duration_details"]["ten_year"].get("max_drawdown_percent"),
                "mdd_max_drawdown_percent": metrics["drawdown"]["mdd_duration_details"]["max"].get("max_drawdown_percent"),
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
        session.commit()

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


"""Bulk insert or update complete mutual fund analytics JSON"""
def bulk_upsert_analytics(data: list[dict], batch_id: int):
    session = get_session()
    try:
        rows = [
            {
                "scheme_code": item["meta"]["scheme_code"],
                "batch_id": batch_id,
                "full_data": item
            }
            for item in data
        ]

        if not rows:
            return

        stmt = insert(SchemeAnalyticsORM).values(rows)

        stmt = stmt.on_conflict_do_update(
            index_elements=["scheme_code"],
            set_={
                "batch_id": stmt.excluded.batch_id,
                "updated_at": func.now(),
                "full_data": stmt.excluded.full_data
            }
        )

        session.execute(stmt)
        session.commit()

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


"""Creates a new batch entry"""
def create_batch(session):
    batch = BatchORM(batch_name="daily_run")
    session.add(batch)
    session.flush()
    return batch.id


# if __name__ == "__main__":
#     session = get_session()
#     try:
#         batch_id = create_batch(session)
#         session.commit()
#     finally:
#         session.close()

#     BATCH_SIZE = 500

#     for i in range(0, len(data), BATCH_SIZE):
#         chunk = data[i:i + BATCH_SIZE]
#         bulk_upsert_schema(chunk, batch_id)
#         bulk_upsert_analytics(chunk, batch_id)