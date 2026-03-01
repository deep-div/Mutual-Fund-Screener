from sqlalchemy import Column, Integer, String, Float, Date, BigInteger, ForeignKey
from app.db.session import Base, SCHEMA_NAME_MF
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import DateTime
from sqlalchemy.sql import func

TABLE_NAME_1 = "mutual_fund_screener"
TABLE_NAME_2 = "mutual_fund_analytics"

"""Defines single mutual fund screener table"""
class SchemeMetaORM(Base):
    __tablename__ = TABLE_NAME_1
    __table_args__ = {"schema": SCHEMA_NAME_MF}
    
    id = Column(Integer, primary_key=True, index=True)
    scheme_code = Column(BigInteger, unique=True, index=True, nullable=False)
    # Meta Information
    instrument_type = Column(String)
    fund_house = Column(String)
    scheme_name = Column(String)
    scheme_sub_name = Column(String)
    option_type = Column(String)
    plan_type = Column(String)
    scheme_category = Column(String)
    scheme_class = Column(String)
    scheme_sub_category = Column(String)
    scheme_type = Column(String)
    launch_date = Column(Date)
    current_date = Column(Date)
    current_nav = Column(Float)
    time_since_inception_years = Column(Float)
    total_active_days = Column(Integer)
    nav_record_count = Column(Integer)
    isin_growth = Column(String)
    isin_div_reinvestment = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    # Absolute Returns (Short-Term)
    abs_1w = Column(Float)
    abs_1m = Column(Float)
    abs_3m = Column(Float)
    abs_6m = Column(Float)

    # CAGR
    cagr_1y = Column(Float)
    cagr_2y = Column(Float)
    cagr_3y = Column(Float)
    cagr_4y = Column(Float)
    cagr_5y = Column(Float)
    cagr_7y = Column(Float)
    cagr_10y = Column(Float)

    # SIP XIRR
    sip_xirr_1y = Column(Float)
    sip_xirr_2y = Column(Float)
    sip_xirr_3y = Column(Float)
    sip_xirr_4y = Column(Float)
    sip_xirr_5y = Column(Float)
    sip_xirr_7y = Column(Float)
    sip_xirr_10y = Column(Float)

    # Rolling CAGR (Average Only)
    rolling_avg_1y = Column(Float)
    rolling_avg_2y = Column(Float)
    rolling_avg_3y = Column(Float)
    rolling_avg_4y = Column(Float)
    rolling_avg_5y = Column(Float)
    rolling_avg_7y = Column(Float)
    rolling_avg_10y = Column(Float)

    # Risk Metrics (Max Window)
    volatility_max = Column(Float)
    downside_deviation_max = Column(Float)
    skewness_max = Column(Float)
    kurtosis_max = Column(Float)

    # Risk Adjusted Returns (Max Window)
    sharpe_max = Column(Float)
    sortino_max = Column(Float)
    calmar_max = Column(Float)
    pain_index_max = Column(Float)
    ulcer_index_max = Column(Float)

    # Drawdown
    current_drawdown_percent = Column(Float)
    mdd_max_drawdown_percent = Column(Float)
    mdd_one_year_pct = Column(Float)
    mdd_two_year_pct = Column(Float)
    mdd_three_year_pct = Column(Float)
    mdd_four_year_pct = Column(Float)
    mdd_five_year_pct = Column(Float)
    mdd_seven_year_pct = Column(Float)
    mdd_ten_year_pct = Column(Float)


"""Stores complete mutual fund output JSON"""
class SchemeAnalyticsORM(Base):
    __tablename__ = TABLE_NAME_2
    __table_args__ = {"schema": SCHEMA_NAME_MF}

    id = Column(Integer, primary_key=True, index=True)
    scheme_code = Column(
        BigInteger,
        ForeignKey(f"{SCHEMA_NAME_MF}.{TABLE_NAME_1}.scheme_code", ondelete="CASCADE"),
        unique=True,
        index=True,
        nullable=False
    )
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    full_data = Column(JSONB)
