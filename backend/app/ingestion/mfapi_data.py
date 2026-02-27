import requests
import asyncio
import aiohttp
import nest_asyncio
import re
from typing import List, Dict, Any
from datetime import datetime, timedelta
from app.ingestion.schemas import (
    InstrumentType,
    MutualFundNavResponse,
    SchemeMeta,
    SchemeClass,
    OptionType,
    PlanType,
)
from app.shared.logger import logger

nest_asyncio.apply()


class MFAPIFetcher:
    """Fetch filtered latest schemes and fetch scheme-wise NAV data asynchronously."""

    def __init__(self, base_url: str = "https://api.mfapi.in/mf", max_concurrent: int = 50):
        """Initialize API base url and concurrency control."""
        self.base_url = base_url
        self.max_concurrent = max_concurrent
        logger.info("MFAPIFetcher initialized")

    def fetch_recent_active_schemes(self, days: int) -> List[Dict[str, Any]]:
        """Return Direct Growth Open Ended funds updated within given days."""
        url = f"{self.base_url}/latest"
        logger.info(f"Fetching latest schemes (last {days} days)")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.error(f"Failed to fetch latest schemes: {e}")
            return []

        funds = []
        cutoff_date = datetime.now() - timedelta(days=days)

        for item in data:
            try:
                scheme_type = item.get("schemeType", "")
                scheme_name = item.get("schemeName", "")
                scheme_category = item.get("schemeCategory", "")
                scheme_code = item.get("schemeCode")
                date_str = item.get("date", "")

                if scheme_type != "Open Ended Schemes":
                    continue

                name_lower = scheme_name.lower()

                if "direct" not in name_lower:
                    continue

                if "growth" not in name_lower:
                    continue

                if any(x in name_lower for x in [
                    "regular", "idcw", "bonus", "segregated", "unclaimed"
                ]):
                    continue

                try:
                    parsed_date = datetime.strptime(date_str, "%d-%m-%Y")
                except ValueError:
                    logger.warning(f"Invalid date format for scheme_code={scheme_code}")
                    continue

                if parsed_date >= cutoff_date:
                    funds.append({
                        "scheme_code": scheme_code,
                        "scheme_name": scheme_name,
                        "scheme_category": scheme_category
                    })

            except Exception as e:
                logger.warning(f"Skipping scheme due to processing error: {e}")
                continue

        logger.info(f"Filtered {len(funds)} eligible schemes")
        return funds

    def _build_scheme_meta(self, raw: dict) -> SchemeMeta:
        """Derive and construct SchemeMeta from raw meta dictionary."""

        meta_raw = raw.get("meta", {})

        scheme_name = meta_raw.get("scheme_name") or meta_raw.get("schemeName")
        scheme_category = meta_raw.get("scheme_category") or meta_raw.get("schemeCategory") or ""
        fund_house = meta_raw.get("fund_house") or meta_raw.get("fundHouse")
        scheme_code = meta_raw.get("scheme_code") or meta_raw.get("schemeCode")

        name_lower = (scheme_name or "").lower()
        scheme_sub_name = self._extract_scheme_sub_name(scheme_name)

        # Compute launch_date, current_date, total_active_days from NAV history
        nav_data = raw.get("data", [])

        if not nav_data:
            raise ValueError("NAV data empty")

        sorted_nav = sorted(
            nav_data,
            key=lambda x: datetime.strptime(x["date"], "%d-%m-%Y")
        )

        launch_date = datetime.strptime(sorted_nav[0]["date"], "%d-%m-%Y")
        current_date = datetime.strptime(sorted_nav[-1]["date"], "%d-%m-%Y")
        total_active_days = (current_date - launch_date).days
        nav_record_count = len(nav_data)

        # Asset Class Mapping
        scheme_class = SchemeClass.OTHER.value
        scheme_sub_category = scheme_category

        if " - " in scheme_category:
            left, right = scheme_category.split(" - ", 1)
            scheme_sub_category = right.strip()
            left = left.strip().lower()
            if "equity" in left:
                scheme_class = SchemeClass.EQUITY.value
            elif "debt" in left:
                scheme_class = SchemeClass.DEBT.value
            elif "hybrid" in left:
                scheme_class = SchemeClass.HYBRID.value
            else:
                scheme_class = SchemeClass.OTHER.value

        # Option Type
        if "growth" in name_lower:
            option_type = OptionType.GROWTH.value
        elif "idcw" in name_lower:
            option_type = OptionType.IDCW.value
        elif "bonus" in name_lower:
            option_type = OptionType.BONUS.value
        else:
            option_type = OptionType.OTHER.value

        # Plan Type
        if "direct" in name_lower:
            plan_type = PlanType.DIRECT.value
        elif "regular" in name_lower:
            plan_type = PlanType.REGULAR.value
        else:
            plan_type = PlanType.OTHER.value

        return SchemeMeta(
            instrument_type=InstrumentType.MUTUAL_FUND.value,
            scheme_code=scheme_code,
            fund_house=fund_house,
            scheme_class=scheme_class,
            scheme_category=scheme_category,
            scheme_sub_category=scheme_sub_category,
            scheme_name=scheme_name,
            scheme_sub_name=scheme_sub_name,
            launch_date=launch_date,
            current_date=current_date,
            total_active_days=total_active_days,
            nav_record_count=nav_record_count,
            option_type=option_type,
            plan_type=plan_type,
            scheme_type=meta_raw.get("scheme_type") or meta_raw.get("schemeType"),
            isin_growth=meta_raw.get("isin_growth") or meta_raw.get("isinGrowth"),
            isin_div_reinvestment=meta_raw.get("isin_div_reinvestment") or meta_raw.get("isinDivReinvestment"),
        )

    @staticmethod
    def _extract_scheme_sub_name(scheme_name: str) -> str:
        """Return text before first hyphen as base scheme name."""
        if not scheme_name:
            return ""
        cleaned = scheme_name.strip()
        if "-" in cleaned:
            return cleaned.split("-", 1)[0].strip()
        return cleaned
    
    async def fetch_scheme(self, session, semaphore, scheme_code):
        """Fetch NAV data, enrich meta from raw, then validate full response."""
        async with semaphore:
            try:
                async with session.get(f"{self.base_url}/{scheme_code}", timeout=10) as response:
                    if response.status != 200:
                        logger.warning(f"Non-200 response for scheme_code={scheme_code}")
                        return None

                    raw = await response.json()

                    try:
                        # STEP 1: Enrich meta directly from raw
                        enriched_meta = self._build_scheme_meta(raw)

                        # STEP 2: Inject enriched meta
                        raw["meta"] = enriched_meta.model_dump(mode="json")

                        # STEP 3: Final validation (only once)
                        validated = MutualFundNavResponse.model_validate(raw)

                        return validated.model_dump(mode="json")

                    except Exception as e:
                        logger.warning(f"Validation failed for scheme_code={scheme_code}: {e}")
                        return None

            except Exception as e:
                logger.error(f"Error fetching scheme_code={scheme_code}: {e}")
                return None

    async def fetch_schemes_from_list(self, schemes_list):
        """Extract scheme_code from dict list and fetch NAV data."""
        connector = aiohttp.TCPConnector(limit=100)
        timeout = aiohttp.ClientTimeout(total=30)

        scheme_codes = [
            item["scheme_code"]
            for item in schemes_list
            if isinstance(item.get("scheme_code"), int) and item["scheme_code"] > 0
        ]
        logger.info(f"Starting NAV fetch for {len(scheme_codes)} schemes")

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            semaphore = asyncio.Semaphore(self.max_concurrent)

            tasks = [
                self.fetch_scheme(session, semaphore, code)
                for code in scheme_codes
            ]

            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"Unexpected async execution error: {e}")
                return []

            final_results = []
            failed_count = 0

            for result in results:
                if isinstance(result, Exception):
                    failed_count += 1
                    continue
                if result is not None:
                    final_results.append(result)
                else:
                    failed_count += 1

            logger.info(
                f"NAV fetch completed | Success: {len(final_results)} | Failed/Skipped: {failed_count}"
            )

            return final_results

# if __name__ == "__main__":
#     try:
#         fetcher = MFAPIFetcher(max_concurrent=50)
#         days = 7
#         schemes_list = fetcher.fetch_recent_active_schemes(days)
#         data = asyncio.run(fetcher.fetch_schemes_from_list(schemes_list[:10]))
#         logger.info("Execution completed successfully")
#     except Exception as e:
#         logger.error(f"Fatal error in main execution: {e}")
