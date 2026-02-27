from typing import Dict, Optional, List
from pydantic import BaseModel, Field, ConfigDict


class DrawdownDetails(BaseModel):
    max_drawdown_percent: float
    peak_date: Optional[str]
    peak_nav: Optional[float]
    trough_date: Optional[str]
    trough_nav: Optional[float]
    recovery_date: Optional[str]
    recovery_nav: Optional[float]
    drawdown_duration_days: int
    drawdown_duration_navs: int
    recovery_duration_days: Optional[int]
    recovery_duration_navs: Optional[int]


class SipMetrics(BaseModel):
    monthly_amount: int
    total_invested: float
    current_value: float
    absolute_return_percent: float
    xirr_percent: float


class YearConsistency(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    year: Optional[int]
    return_: float = Field(alias="return")


class MonthConsistency(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    month: Optional[str]
    return_: float = Field(alias="return")


class DayConsistency(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    date: Optional[str]
    return_: float = Field(alias="return")


class ConsistencyMetrics(BaseModel):
    positive_years_percent: float
    positive_months_percent: float
    positive_days_percent: float
    best_year: YearConsistency
    worst_year: YearConsistency
    best_month: MonthConsistency
    worst_month: MonthConsistency
    best_day: DayConsistency
    worst_day: DayConsistency


class RollingSummary(BaseModel):
    average: float
    median: float
    maximum: float
    minimum: float
    positive_percent: float
    observations: int


class RollingPoint(BaseModel):
    date: str
    cagr_percent: float


class RollingCagrPeriod(BaseModel):
    summary: RollingSummary
    points: List[RollingPoint]


class NavMetricsOutput(BaseModel):
    absolute_returns_percent: Dict[str, float]
    cagr_percent: Dict[str, float]

    mdd_duration_details: Dict[str, DrawdownDetails]
    yearly_mdd_last_10_years: Dict[str, DrawdownDetails]
    year_on_year_percent: Dict[str, float]

    volatility_annualized_percent: Dict[str, float]
    sharpe_ratio: Dict[str, float]
    calmar_ratio: Dict[str, float]
    sortino_ratio: Dict[str, float]
    downside_deviation_percent: Dict[str, float]
    skewness: Dict[str, float]
    kurtosis: Dict[str, float]

    consistency: ConsistencyMetrics
    sip_returns: Dict[str, SipMetrics]

    rolling_cagr_percent: Optional[Dict[str, RollingCagrPeriod]] = None