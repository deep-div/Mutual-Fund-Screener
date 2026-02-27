from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import List, Optional
from datetime import datetime, date
from decimal import Decimal
from enum import Enum

class InstrumentType(str, Enum):
    """Financial instrument type enum."""
    MUTUAL_FUND = "Mutual Fund"
    STOCK = "Stock"
    ETF = "ETF"
    INDEX = "Index"

class AssetClass(str, Enum):
    """Asset class enum."""
    EQUITY = "Equity"
    DEBT = "Debt"
    HYBRID = "Hybrid"
    OTHER = "Other"


class PlanType(str, Enum):
    """Plan type enum."""
    DIRECT = "Direct"
    REGULAR = "Regular"


class OptionType(str, Enum):
    """Option type enum."""
    GROWTH = "Growth"
    IDCW = "IDCW"
    BONUS = "Bonus"


class SchemeType(str, Enum):
    """Scheme structure type enum."""
    OPEN_ENDED = "Open Ended Schemes"
    CLOSE_ENDED = "Close Ended Schemes"
    INTERVAL = "Interval Fund"


class SchemeMeta(BaseModel):
    instrument_type: InstrumentType
    scheme_code: int
    fund_house: str
    scheme_name: str
    scheme_category: str
    asset_class: AssetClass
    scheme_sub_category: str
    option_type: Optional[OptionType] = None
    plan_type: Optional[PlanType] = None
    scheme_type: Optional[SchemeType] = None
    isin_growth: Optional[str] = None
    isin_div_reinvestment: Optional[str] = None


class NavPoint(BaseModel):
    """Single NAV datapoint for a given date."""
    date: date
    nav: Decimal

    @field_validator("date", mode="before")
    @classmethod
    def parse_date(cls, v):
        """Parse DD-MM-YYYY formatted date string."""
        if isinstance(v, str):
            return datetime.strptime(v, "%d-%m-%Y").date()
        return v

    @field_validator("nav", mode="before")
    @classmethod
    def parse_nav(cls, v):
        """Convert NAV string to Decimal."""
        if isinstance(v, str):
            return Decimal(v)
        return v


class MutualFundNavResponse(BaseModel):
    """Complete mutual fund NAV ingestion response."""
    meta: SchemeMeta
    data: List[NavPoint]