from pydantic import BaseModel, field_validator
from typing import List, Optional
from datetime import datetime, date
from decimal import Decimal
from enum import Enum

class SchemeSubCategory(str, Enum):
    """SEBI mutual fund scheme category enum."""
    #  Equity 
    LARGE_CAP = "Large Cap Fund"
    MID_CAP = "Mid Cap Fund"
    SMALL_CAP = "Small Cap Fund"
    LARGE_MID_CAP = "Large & Mid Cap Fund"
    MULTI_CAP = "Multi Cap Fund"
    FLEXI_CAP = "Flexi Cap Fund"
    FOCUSED = "Focused Fund"
    ELSS = "ELSS"
    VALUE = "Value Fund"
    CONTRA = "Contra Fund"
    DIVIDEND_YIELD = "Dividend Yield Fund"
    SECTORAL_THEMATIC = "Sectoral/ Thematic"
    INDEX = "Index Funds"

    #  Debt 
    OVERNIGHT = "Overnight Fund"
    LIQUID = "Liquid Fund"
    ULTRA_SHORT_DURATION = "Ultra Short Duration Fund"
    LOW_DURATION = "Low Duration Fund"
    MONEY_MARKET = "Money Market Fund"
    SHORT_DURATION = "Short Duration Fund"
    MEDIUM_DURATION = "Medium Duration Fund"
    MEDIUM_TO_LONG_DURATION = "Medium to Long Duration Fund"
    LONG_DURATION = "Long Duration Fund"
    DYNAMIC_BOND = "Dynamic Bond"
    CORPORATE_BOND = "Corporate Bond Fund"
    BANKING_PSU = "Banking and PSU Fund"
    CREDIT_RISK = "Credit Risk Fund"
    FLOATER = "Floater Fund"
    Gilt_Fund = "Gilt Fund"

    #  Hybrid 
    AGGRESSIVE_HYBRID = "Aggressive Hybrid Fund"
    BALANCED_HYBRID = "Balanced Hybrid Fund"
    CONSERVATIVE_HYBRID = "Conservative Hybrid Fund"
    DYNAMIC_ASSET_ALLOCATION = "Balanced Advantage"
    EQUITY_SAVINGS = "Equity Savings"
    MULTI_ASSET_ALLOCATION = "Multi Asset Allocation"
    ARBITRAGE = "Arbitrage Fund"

#  Others 
    RETIREMENT = "Retirement Fund"
    CHILDRENS = "Children’s Fund"
    FOF_DOMESTIC = "FoF Domestic"
    FOF_OVERSEAS = "FoF Overseas"

class InstrumentType(str, Enum):
    """Financial instrument type enum."""
    MUTUAL_FUND = "Mutual Fund"
    STOCK = "Stock"
    ETF = "ETF"
    INDEX = "Index"

class SchemeClass(str, Enum):
    """Asset class enum."""
    EQUITY = "Equity"
    DEBT = "Debt"
    HYBRID = "Hybrid"
    OTHER = "Other"


class PlanType(str, Enum):
    """Plan type enum."""
    DIRECT = "Direct"
    REGULAR = "Regular"
    OTHER = "Other"

class OptionType(str, Enum):
    """Option type enum."""
    GROWTH = "Growth"
    IDCW = "IDCW"
    BONUS = "Bonus"
    OTHER = "Other"

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
    scheme_sub_name: str
    option_type: OptionType
    plan_type: PlanType
    scheme_category: str
    scheme_class: SchemeClass
    scheme_sub_category: SchemeSubCategory | str
    launch_date: date
    current_date: date
    current_nav: float
    total_active_days: int
    nav_record_count: int
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
