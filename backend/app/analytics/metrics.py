import json
from datetime import timedelta
from app.shared.logger import logger
from datetime import datetime

class NavMetrics:
    """Compute absolute return, CAGR, MDD, YoY and Rolling CAGR from NAV history"""

    def __init__(self, nav_data):
        """Initialize NAV data sorted ascending with parsed dates"""
        try:
            if not nav_data:
                raise ValueError("NAV data is empty")

            parsed_data = []
            for entry in nav_data:
                parsed_data.append({
                    "date": datetime.strptime(entry["date"], "%Y-%m-%d").date(),
                    "nav": float(entry["nav"])
                })

            self.nav_data = sorted(parsed_data, key=lambda x: x['date'])
            logger.info("NavMetrics initialized successfully")

        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            raise

    def _get_nav_for_period(self, days):
        """Get NAV for first date <= target date"""
        latest_date = self.nav_data[-1]['date']
        target_date = latest_date - timedelta(days=days)

        for entry in reversed(self.nav_data):
            if entry['date'] <= target_date:
                return entry['nav'], entry['date']

        return self.nav_data[0]['nav'], self.nav_data[0]['date']

    def _absolute_return(self, past_nav):
        """Calculate absolute return percentage"""
        latest_nav = self.nav_data[-1]['nav']
        return round(((latest_nav - past_nav) / past_nav) * 100, 2)

    def _cagr(self, past_nav, past_date):
        """Calculate CAGR percentage"""
        latest_nav = self.nav_data[-1]['nav']
        latest_date = self.nav_data[-1]['date']

        years = (latest_date - past_date).days / 365.25
        if years <= 0:
            return 0.0

        value = ((latest_nav / past_nav) ** (1 / years) - 1) * 100
        return round(value, 2)

    def _mdd(self, start_date):
        """Calculate maximum drawdown percentage"""
        filtered = [e for e in self.nav_data if e['date'] >= start_date]
        if not filtered:
            return 0.0

        max_nav = filtered[0]['nav']
        max_drawdown = 0.0

        for entry in filtered:
            nav = entry['nav']
            if nav > max_nav:
                max_nav = nav

            drawdown = (nav - max_nav) / max_nav
            if drawdown < max_drawdown:
                max_drawdown = drawdown

        return round(max_drawdown * 100, 2)

    def _year_on_year_returns(self):
        """Calculate calendar year returns percentage"""
        year_map = {}

        for entry in self.nav_data:
            year = entry['date'].year
            nav = entry['nav']

            if year not in year_map:
                year_map[year] = {"start": nav, "end": nav}
            else:
                year_map[year]["end"] = nav

        yoy = {}
        for year, values in year_map.items():
            start_nav = values["start"]
            end_nav = values["end"]
            if start_nav != 0:
                yoy[str(year)] = round(((end_nav - start_nav) / start_nav) * 100, 2)

        return yoy

    def _rolling_cagr_all_periods(self):
        """Compute monthly rolling CAGR using first NAV of each month as window start"""
        try:
            periods = [1, 2, 3, 4, 5, 7, 10]
            results = {}

            n = len(self.nav_data)
            if n < 2:
                return {}

            dates = [e["date"] for e in self.nav_data]
            navs = [e["nav"] for e in self.nav_data]

            # Identify first trading day of each month
            monthly_indices = []
            seen = set()

            for i, d in enumerate(dates):
                key = (d.year, d.month)
                if key not in seen:
                    seen.add(key)
                    monthly_indices.append(i)

            for years in periods:
                window_days = int(years * 365.25)
                rolling_values = []
                rolling_points = []

                j = 0

                for i in monthly_indices:
                    start_date = dates[i]
                    start_nav = navs[i]
                    target_date = start_date + timedelta(days=window_days)

                    while j < n and dates[j] < target_date:
                        j += 1

                    if j >= n:
                        break

                    end_date = dates[j]
                    end_nav = navs[j]

                    actual_years = (end_date - start_date).days / 365.25

                    if actual_years > 0:
                        cagr = ((end_nav / start_nav) ** (1 / actual_years) - 1) * 100

                        rolling_values.append(cagr)

                        rolling_points.append({
                            "date": end_date.isoformat(),   # Better for graph X-axis
                            "cagr_percent": round(cagr, 4)
                        })

                if rolling_values:
                    results[f"{years}_year"] = {
                        "summary": {
                            "average": round(sum(rolling_values) / len(rolling_values), 4),
                            "max": round(max(rolling_values), 4),
                            "min": round(min(rolling_values), 4),
                            "positive_ratio_percent": round(
                                (sum(1 for x in rolling_values if x > 0) / len(rolling_values)) * 100,
                                2
                            ),
                            "observations": len(rolling_values)
                        },
                        "points": rolling_points
                    }

            logger.info("Monthly rolling CAGR calculated successfully")
            return results

        except Exception as e:
            logger.error(f"Monthly rolling CAGR calculation failed: {str(e)}")
            raise

    def get_all_metrics(self):
        """Return all metrics in dict format"""
        try:
            periods = {
                "one_year": 365,
                "two_year": 730,
                "three_year": 1095,
                "four_year": 1460,
                "five_year": 1825,
                "seven_year": 2555,
                "ten_year": 3650,
            }

            absolute_returns = {}
            cagr_returns = {}
            mdd_returns = {}

            for name, days in periods.items():
                past_nav, past_date = self._get_nav_for_period(days)
                absolute_returns[name] = self._absolute_return(past_nav)
                cagr_returns[name] = self._cagr(past_nav, past_date)
                mdd_returns[name] = self._mdd(past_date)

            launch_nav = self.nav_data[0]['nav']
            launch_date = self.nav_data[0]['date']

            absolute_returns["max"] = self._absolute_return(launch_nav)
            cagr_returns["max"] = self._cagr(launch_nav, launch_date)
            mdd_returns["max"] = self._mdd(launch_date)

            result = {
                "absolute_returns_percent": absolute_returns,
                "cagr_percent": cagr_returns,
                "mdd_percent": mdd_returns,
                "year_on_year_percent": self._year_on_year_returns(),
                # "rolling_cagr_percent": self._rolling_cagr_all_periods()
            }

            logger.info("NAV metrics calculated successfully")
            return result

        except Exception as e:
            logger.error(f"Metric calculation failed: {str(e)}")
            raise


# # Usage
# metrics = NavMetrics(data[5]['data'])
# response_dict = metrics.get_all_metrics()