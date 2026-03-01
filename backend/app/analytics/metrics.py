import json
import bisect
from datetime import timedelta
from app.shared.logger import logger
from datetime import datetime
from app.analytics.schemas import NavMetricsOutput

class NavMetrics:
    """Compute absolute return, CAGR, MDD, YoY and Rolling CAGR from NAV history"""

    def __init__(self, nav_data):
        """Initialize NAV data sorted ascending with parsed dates"""
        try:
            if not nav_data:
                raise ValueError("NAV data is empty")

            parsed_data = []
            for entry in nav_data:
                nav_value = float(entry["nav"])
                if nav_value <= 0:
                    raise ValueError(f"Invalid NAV value for {entry['date']}: {entry['nav']}")
                parsed_data.append({
                    "date": datetime.strptime(entry["date"], "%Y-%m-%d").date(),
                    "nav": nav_value
                })

            self.nav_data = sorted(parsed_data, key=lambda x: x['date'])

        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            raise

    def _get_nav_for_period(self, days):
        """Get NAV for first available date on/after target lookback date"""
        latest_date = self.nav_data[-1]['date']
        target_date = latest_date - timedelta(days=days)

        for entry in self.nav_data:
            if entry['date'] >= target_date:
                return entry['nav'], entry['date']

        return self.nav_data[0]['nav'], self.nav_data[0]['date']

    def _absolute_return(self, past_nav):
        """Calculate absolute return percentage"""
        latest_nav = self.nav_data[-1]['nav']
        if past_nav <= 0:
            return 0.0
        return round(((latest_nav - past_nav) / past_nav) * 100, 2)

    def _cagr(self, past_nav, past_date):
        """Calculate CAGR percentage"""
        latest_nav = self.nav_data[-1]['nav']
        latest_date = self.nav_data[-1]['date']

        if past_nav <= 0:
            return 0.0

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
    def _mdd_duration_details(self, start_date):
        """Calculate max drawdown details for a period with duration and recovery"""
        filtered = [e for e in self.nav_data if e['date'] >= start_date]
        if len(filtered) < 2:
            if not filtered:
                return {
                    "max_drawdown_percent": 0.0,
                    "peak_date": None,
                    "peak_nav": None,
                    "trough_date": None,
                    "trough_nav": None,
                    "recovery_date": None,
                    "recovery_nav": None,
                    "drawdown_duration_days": 0,
                    "drawdown_duration_navs": 0,
                    "recovery_duration_days": None,
                    "recovery_duration_navs": None
                }
            d = filtered[0]['date'].isoformat()
            return {
                "max_drawdown_percent": 0.0,
                "peak_date": d,
                "peak_nav": round(filtered[0]['nav'], 4),
                "trough_date": d,
                "trough_nav": round(filtered[0]['nav'], 4),
                "recovery_date": d,
                "recovery_nav": round(filtered[0]['nav'], 4),
                "drawdown_duration_days": 0,
                "drawdown_duration_navs": 0,
                "recovery_duration_days": 0,
                "recovery_duration_navs": 0
            }

        peak_nav = filtered[0]['nav']
        peak_idx = 0
        max_dd = 0.0
        trough_idx = 0
        dd_peak_idx = 0

        for i, entry in enumerate(filtered):
            nav = entry['nav']
            if nav > peak_nav:
                peak_nav = nav
                peak_idx = i

            drawdown = (nav - peak_nav) / peak_nav
            if drawdown < max_dd:
                max_dd = drawdown
                trough_idx = i
                dd_peak_idx = peak_idx

        peak_date = filtered[dd_peak_idx]['date']
        trough_date = filtered[trough_idx]['date']
        target_nav = filtered[dd_peak_idx]['nav']

        recovery_idx = None
        for i in range(trough_idx + 1, len(filtered)):
            if filtered[i]['nav'] >= target_nav:
                recovery_idx = i
                break

        if recovery_idx is not None:
            recovery_date = filtered[recovery_idx]['date']
            recovery_days = (recovery_date - trough_date).days
            recovery_navs = recovery_idx - trough_idx
            recovery_date_val = recovery_date.isoformat()
        else:
            recovery_days = None
            recovery_navs = None
            recovery_date_val = None

        return {
            "max_drawdown_percent": round(max_dd * 100, 2),
            "peak_date": peak_date.isoformat(),
            "peak_nav": round(filtered[dd_peak_idx]['nav'], 4),
            "trough_date": trough_date.isoformat(),
            "trough_nav": round(filtered[trough_idx]['nav'], 4),
            "recovery_date": recovery_date_val,
            "recovery_nav": round(filtered[recovery_idx]['nav'], 4) if recovery_idx is not None else None,
            "drawdown_duration_days": (trough_date - peak_date).days,
            "drawdown_duration_navs": trough_idx - dd_peak_idx,
            "recovery_duration_days": recovery_days,
            "recovery_duration_navs": recovery_navs
        }
    def _current_drawdown_details(self):
        """Calculate current drawdown from latest NAV against running peak"""
        if not self.nav_data:
            return {
                "max_drawdown_percent": 0.0,
                "peak_date": None,
                "peak_nav": None,
                "trough_date": None,
                "trough_nav": None,
                "recovery_date": None,
                "recovery_nav": None,
                "drawdown_duration_days": 0,
                "drawdown_duration_navs": 0,
                "recovery_duration_days": None,
                "recovery_duration_navs": None,
            }

        peak_nav = self.nav_data[0]["nav"]
        peak_idx = 0
        for i, entry in enumerate(self.nav_data):
            if entry["nav"] >= peak_nav:
                peak_nav = entry["nav"]
                peak_idx = i

        latest_idx = len(self.nav_data) - 1
        latest_entry = self.nav_data[latest_idx]
        peak_entry = self.nav_data[peak_idx]

        current_dd = ((latest_entry["nav"] - peak_nav) / peak_nav) * 100 if peak_nav != 0 else 0.0
        is_recovered = latest_entry["nav"] >= peak_nav

        return {
            "max_drawdown_percent": round(current_dd, 2),
            "peak_date": peak_entry["date"].isoformat(),
            "peak_nav": round(peak_entry["nav"], 4),
            "trough_date": latest_entry["date"].isoformat(),
            "trough_nav": round(latest_entry["nav"], 4),
            "recovery_date": latest_entry["date"].isoformat() if is_recovered else None,
            "recovery_nav": round(latest_entry["nav"], 4) if is_recovered else None,
            "drawdown_duration_days": (latest_entry["date"] - peak_entry["date"]).days,
            "drawdown_duration_navs": latest_idx - peak_idx,
            "recovery_duration_days": 0 if is_recovered else None,
            "recovery_duration_navs": 0 if is_recovered else None,
        }
    def _yearly_mdd_last_10_years(self):
        """Calculate year-wise MDD details for last 10 calendar years"""
        latest_year = self.nav_data[-1]['date'].year
        start_year = latest_year - 9
        results = {}

        for year in range(start_year, latest_year + 1):
            entries = [e for e in self.nav_data if e['date'].year == year]

            if len(entries) < 2:
                if not entries:
                    results[str(year)] = {
                        "max_drawdown_percent": 0.0,
                        "peak_date": None,
                        "peak_nav": None,
                        "trough_date": None,
                        "trough_nav": None,
                        "recovery_date": None,
                        "recovery_nav": None,
                        "drawdown_duration_days": 0,
                        "drawdown_duration_navs": 0,
                        "recovery_duration_days": None,
                        "recovery_duration_navs": None
                    }
                else:
                    d = entries[0]['date'].isoformat()
                    nav = round(entries[0]['nav'], 4)
                    results[str(year)] = {
                        "max_drawdown_percent": 0.0,
                        "peak_date": d,
                        "peak_nav": nav,
                        "trough_date": d,
                        "trough_nav": nav,
                        "recovery_date": d,
                        "recovery_nav": nav,
                        "drawdown_duration_days": 0,
                        "drawdown_duration_navs": 0,
                        "recovery_duration_days": 0,
                        "recovery_duration_navs": 0
                    }
                continue

            peak_nav = entries[0]['nav']
            peak_idx = 0
            max_dd = 0.0
            trough_idx = 0
            dd_peak_idx = 0

            for i, entry in enumerate(entries):
                nav = entry['nav']
                if nav > peak_nav:
                    peak_nav = nav
                    peak_idx = i

                drawdown = (nav - peak_nav) / peak_nav
                if drawdown < max_dd:
                    max_dd = drawdown
                    trough_idx = i
                    dd_peak_idx = peak_idx

            peak_date = entries[dd_peak_idx]['date']
            trough_date = entries[trough_idx]['date']
            target_nav = entries[dd_peak_idx]['nav']

            recovery_idx = None
            for i in range(trough_idx + 1, len(entries)):
                if entries[i]['nav'] >= target_nav:
                    recovery_idx = i
                    break

            if recovery_idx is not None:
                recovery_date = entries[recovery_idx]['date']
                recovery_days = (recovery_date - trough_date).days
                recovery_navs = recovery_idx - trough_idx
                recovery_date_val = recovery_date.isoformat()
                recovery_nav_val = round(entries[recovery_idx]['nav'], 4)
            else:
                recovery_days = None
                recovery_navs = None
                recovery_date_val = None
                recovery_nav_val = None

            results[str(year)] = {
                "max_drawdown_percent": round(max_dd * 100, 2),
                "peak_date": peak_date.isoformat(),
                "peak_nav": round(entries[dd_peak_idx]['nav'], 4),
                "trough_date": trough_date.isoformat(),
                "trough_nav": round(entries[trough_idx]['nav'], 4),
                "recovery_date": recovery_date_val,
                "recovery_nav": recovery_nav_val,
                "drawdown_duration_days": (trough_date - peak_date).days,
                "drawdown_duration_navs": trough_idx - dd_peak_idx,
                "recovery_duration_days": recovery_days,
                "recovery_duration_navs": recovery_navs
            }

        return results
    def _drawdown_frequency(self):
        """Count years crossing drawdown thresholds using yearly MDD data"""
        yearly_mdd = self._yearly_mdd_last_10_years()

        thresholds = {
            "beyond_5_percent": 5,
            "beyond_10_percent": 10,
            "beyond_20_percent": 20,
            "beyond_30_percent": 30,
            "beyond_40_percent": 40,
        }

        frequency = {
            key: {"count": 0, "years": []}
            for key in thresholds
        }

        for year, details in yearly_mdd.items():
            dd_abs = abs(details.get("max_drawdown_percent", 0.0))
            for key, threshold in thresholds.items():
                if dd_abs >= threshold:
                    frequency[key]["years"].append(year)

        for key in frequency:
            frequency[key]["count"] = len(frequency[key]["years"])

        return frequency
    def _annualized_volatility(self, start_date):
        """Calculate annualized volatility (%) from daily NAV returns"""
        filtered = [e for e in self.nav_data if e['date'] >= start_date]
        if len(filtered) < 2:
            return 0.0

        navs = [e['nav'] for e in filtered]
        daily_returns = []

        for i in range(1, len(navs)):
            prev_nav = navs[i - 1]
            curr_nav = navs[i]
            if prev_nav != 0:
                daily_returns.append((curr_nav / prev_nav) - 1)

        n = len(daily_returns)
        if n < 2:
            return 0.0

        mean_return = sum(daily_returns) / n
        variance = sum((r - mean_return) ** 2 for r in daily_returns) / (n - 1)
        annualized_vol = (variance ** 0.5) * (252 ** 0.5) * 100
        return round(annualized_vol, 2)
    def _sharpe_ratio(self, start_date, risk_free_rate_annual=0.0):
        """Calculate annualized Sharpe ratio from daily NAV returns"""
        filtered = [e for e in self.nav_data if e['date'] >= start_date]
        if len(filtered) < 2:
            return 0.0

        navs = [e['nav'] for e in filtered]
        daily_returns = []

        for i in range(1, len(navs)):
            prev_nav = navs[i - 1]
            curr_nav = navs[i]
            if prev_nav != 0:
                daily_returns.append((curr_nav / prev_nav) - 1)

        n = len(daily_returns)
        if n < 2:
            return 0.0

        rf_daily = risk_free_rate_annual / 252
        excess_returns = [r - rf_daily for r in daily_returns]

        mean_excess = sum(excess_returns) / n
        variance = sum((r - mean_excess) ** 2 for r in excess_returns) / (n - 1)
        std_dev = variance ** 0.5

        if std_dev == 0:
            return 0.0

        sharpe = (mean_excess / std_dev) * (252 ** 0.5)
        return round(sharpe, 4)

    def _calendar_year_return_tuples(self):
        """Calculate calendar year returns using previous year's last NAV as base"""
        year_map = {}

        for entry in self.nav_data:
            year = entry['date'].year
            nav = entry['nav']
            if year not in year_map:
                year_map[year] = {"first": nav, "last": nav}
            else:
                year_map[year]["last"] = nav

        returns = []
        years = sorted(year_map.keys())

        for idx, year in enumerate(years):
            if idx == 0:
                start_nav = year_map[year]["first"]
            else:
                prev_year = years[idx - 1]
                start_nav = year_map[prev_year]["last"]

            end_nav = year_map[year]["last"]
            if start_nav != 0:
                returns.append((year, ((end_nav - start_nav) / start_nav) * 100))

        return returns
    def _year_on_year_returns(self):
        """Calculate calendar year returns percentage"""
        return {str(year): round(ret, 2) for year, ret in self._calendar_year_return_tuples()}
    def _monthly_return_heatmap(self):
        """Calculate monthly return matrix as year -> month -> return%"""
        month_buckets = {}
        for entry in self.nav_data:
            key = (entry["date"].year, entry["date"].month)
            if key not in month_buckets:
                month_buckets[key] = {"start": entry["nav"], "end": entry["nav"]}
            else:
                month_buckets[key]["end"] = entry["nav"]

        heatmap = {}
        for (year, month), vals in sorted(month_buckets.items()):
            start_nav = vals["start"]
            end_nav = vals["end"]
            ret = 0.0 if start_nav == 0 else round(((end_nav - start_nav) / start_nav) * 100, 2)
            year_key = str(year)
            month_key = f"{month:02d}"
            if year_key not in heatmap:
                heatmap[year_key] = {}
            heatmap[year_key][month_key] = ret

        return heatmap
    def _return_distribution(self):
        """Bucket calendar year returns into readability bands"""
        year_returns = self._year_on_year_returns()

        buckets = {
            "above_50_percent": {"count": 0, "years": []},
            "30_to_50_percent": {"count": 0, "years": []},
            "10_to_30_percent": {"count": 0, "years": []},
            "0_to_10_percent": {"count": 0, "years": []},
            "negative": {"count": 0, "years": []},
        }

        for year, ret in year_returns.items():
            if ret > 50:
                key = "above_50_percent"
            elif ret >= 30:
                key = "30_to_50_percent"
            elif ret >= 10:
                key = "10_to_30_percent"
            elif ret >= 0:
                key = "0_to_10_percent"
            else:
                key = "negative"

            buckets[key]["years"].append(year)

        for key in buckets:
            buckets[key]["count"] = len(buckets[key]["years"])

        return buckets
    def _sortino_ratio(self, start_date, risk_free_rate_annual=0.0):
        """Calculate annualized Sortino ratio from daily NAV returns"""
        filtered = [e for e in self.nav_data if e['date'] >= start_date]
        if len(filtered) < 2:
            return 0.0

        navs = [e['nav'] for e in filtered]
        daily_returns = []

        for i in range(1, len(navs)):
            prev_nav = navs[i - 1]
            curr_nav = navs[i]
            if prev_nav != 0:
                daily_returns.append((curr_nav / prev_nav) - 1)

        n = len(daily_returns)
        if n < 2:
            return 0.0

        rf_daily = risk_free_rate_annual / 252
        excess_returns = [r - rf_daily for r in daily_returns]

        mean_excess = sum(excess_returns) / n
        downside_squared = [(min(0.0, r)) ** 2 for r in excess_returns]
        downside_deviation = (sum(downside_squared) / n) ** 0.5

        if downside_deviation == 0:
            return 0.0

        sortino = (mean_excess / downside_deviation) * (252 ** 0.5)
        return round(sortino, 4)

    def _skewness(self, start_date):
        """Calculate skewness of daily returns for a period"""
        filtered = [e for e in self.nav_data if e['date'] >= start_date]
        if len(filtered) < 3:
            return 0.0

        navs = [e['nav'] for e in filtered]
        daily_returns = []

        for i in range(1, len(navs)):
            prev_nav = navs[i - 1]
            curr_nav = navs[i]
            if prev_nav != 0:
                daily_returns.append((curr_nav / prev_nav) - 1)

        n = len(daily_returns)
        if n < 3:
            return 0.0

        mean_return = sum(daily_returns) / n
        variance = sum((r - mean_return) ** 2 for r in daily_returns) / (n - 1)
        std_dev = variance ** 0.5

        if std_dev == 0:
            return 0.0

        skew = sum(((r - mean_return) / std_dev) ** 3 for r in daily_returns) / n
        return round(skew, 4)

    def _kurtosis(self, start_date):
        """Calculate kurtosis of daily returns for a period"""
        filtered = [e for e in self.nav_data if e['date'] >= start_date]
        if len(filtered) < 4:
            return 0.0

        navs = [e['nav'] for e in filtered]
        daily_returns = []

        for i in range(1, len(navs)):
            prev_nav = navs[i - 1]
            curr_nav = navs[i]
            if prev_nav != 0:
                daily_returns.append((curr_nav / prev_nav) - 1)

        n = len(daily_returns)
        if n < 4:
            return 0.0

        mean_return = sum(daily_returns) / n
        variance = sum((r - mean_return) ** 2 for r in daily_returns) / (n - 1)
        std_dev = variance ** 0.5

        if std_dev == 0:
            return 0.0

        kurt = sum(((r - mean_return) / std_dev) ** 4 for r in daily_returns) / n - 3
        return round(kurt, 4)
    def _downside_deviation_percent(self, start_date, threshold_annual=0.0):
        """Calculate annualized downside deviation (%) from daily returns"""
        filtered = [e for e in self.nav_data if e['date'] >= start_date]
        if len(filtered) < 2:
            return 0.0

        navs = [e['nav'] for e in filtered]
        daily_returns = []

        for i in range(1, len(navs)):
            prev_nav = navs[i - 1]
            curr_nav = navs[i]
            if prev_nav != 0:
                daily_returns.append((curr_nav / prev_nav) - 1)

        n = len(daily_returns)
        if n < 2:
            return 0.0

        threshold_daily = threshold_annual / 252
        downside_squared = [(min(0.0, r - threshold_daily)) ** 2 for r in daily_returns]
        downside_deviation_daily = (sum(downside_squared) / n) ** 0.5
        downside_deviation_annual_percent = downside_deviation_daily * (252 ** 0.5) * 100

        return round(downside_deviation_annual_percent, 2)
    def _calmar_ratio(self, cagr_percent, mdd_percent):
        """Calculate Calmar ratio as CAGR / abs(Max Drawdown)"""
        denominator = abs(mdd_percent)
        if denominator == 0:
            return 0.0

        return round(cagr_percent / denominator, 4)
    def _pain_index(self, start_date):
        """Calculate Pain Index as average drawdown (%) over the period"""
        filtered = [e for e in self.nav_data if e["date"] >= start_date]
        if len(filtered) < 2:
            return 0.0

        peak_nav = filtered[0]["nav"]
        drawdowns = []
        for entry in filtered:
            nav = entry["nav"]
            if nav > peak_nav:
                peak_nav = nav
            dd_pct = 0.0 if peak_nav == 0 else ((peak_nav - nav) / peak_nav) * 100
            drawdowns.append(dd_pct)

        return round(sum(drawdowns) / len(drawdowns), 4)
    def _ulcer_index(self, start_date):
        """Calculate Ulcer Index as RMS drawdown (%) over the period"""
        filtered = [e for e in self.nav_data if e["date"] >= start_date]
        if len(filtered) < 2:
            return 0.0

        peak_nav = filtered[0]["nav"]
        squared_drawdowns = []
        for entry in filtered:
            nav = entry["nav"]
            if nav > peak_nav:
                peak_nav = nav
            dd_pct = 0.0 if peak_nav == 0 else ((peak_nav - nav) / peak_nav) * 100
            squared_drawdowns.append(dd_pct ** 2)

        return round((sum(squared_drawdowns) / len(squared_drawdowns)) ** 0.5, 4)

    def _xnpv(self, rate, cashflows):
        """Calculate NPV for irregular cashflows"""
        if rate <= -0.999999:
            return float("inf")

        first_date = cashflows[0][0]
        total = 0.0

        for dt, amt in cashflows:
            years = (dt - first_date).days / 365.0
            total += amt / ((1 + rate) ** years)

        return total

    def _xirr(self, cashflows):
        """Calculate XIRR using bracketed bisection"""
        if not cashflows:
            return 0.0

        has_pos = any(amt > 0 for _, amt in cashflows)
        has_neg = any(amt < 0 for _, amt in cashflows)
        if not (has_pos and has_neg):
            return 0.0

        low, high = -0.9999, 1.0
        npv_low = self._xnpv(low, cashflows)
        npv_high = self._xnpv(high, cashflows)

        expand_count = 0
        while npv_low * npv_high > 0 and expand_count < 30:
            high *= 2
            npv_high = self._xnpv(high, cashflows)
            expand_count += 1

        if npv_low * npv_high > 0:
            return 0.0

        for _ in range(120):
            mid = (low + high) / 2
            npv_mid = self._xnpv(mid, cashflows)

            if abs(npv_mid) < 1e-7:
                return mid

            if npv_low * npv_mid <= 0:
                high = mid
                npv_high = npv_mid
            else:
                low = mid
                npv_low = npv_mid

        return (low + high) / 2

    def _sip_xirr(self, start_date, sip_amount=1000.0):
        """Calculate SIP metrics using monthly investments"""
        filtered = [e for e in self.nav_data if e['date'] >= start_date]
        if len(filtered) < 2:
            return {
                "monthly_amount": int(sip_amount),
                "total_invested": 0.0,
                "current_value": 0.0,
                "absolute_return_percent": 0.0,
                "xirr_percent": 0.0
            }

        monthly_points = []
        seen = set()

        for entry in filtered:
            key = (entry['date'].year, entry['date'].month)
            if key not in seen:
                seen.add(key)
                monthly_points.append(entry)

        if not monthly_points:
            return {
                "monthly_amount": int(sip_amount),
                "total_invested": 0.0,
                "current_value": 0.0,
                "absolute_return_percent": 0.0,
                "xirr_percent": 0.0
            }

        units = 0.0
        cashflows = []
        total_invested = 0.0

        for entry in monthly_points:
            nav = entry['nav']
            if nav <= 0:
                continue
            units += sip_amount / nav
            total_invested += sip_amount
            cashflows.append((entry['date'], -sip_amount))

        if units <= 0 or total_invested <= 0:
            return {
                "monthly_amount": int(sip_amount),
                "total_invested": round(total_invested, 2),
                "current_value": 0.0,
                "absolute_return_percent": 0.0,
                "xirr_percent": 0.0
            }

        latest_entry = filtered[-1]
        current_value = units * latest_entry['nav']
        cashflows.append((latest_entry['date'], current_value))

        xirr_value = self._xirr(cashflows)
        gain_percent = ((current_value - total_invested) / total_invested) * 100

        return {
            "monthly_amount": int(sip_amount),
            "total_invested": round(total_invested, 2),
            "current_value": round(current_value, 2),
            "absolute_return_percent": round(gain_percent, 2),
            "xirr_percent": round(xirr_value * 100, 2)
        }
    def _consistency_metrics(self):
        """Calculate return consistency across years, months and days"""
        if len(self.nav_data) < 2:
            return {
                "positive_years_percent": 0.0,
                "positive_months_percent": 0.0,
                "positive_days_percent": 0.0,
                "max_consecutive_positive_months": None,
                "max_consecutive_negative_months": None,
                "best_year": {"year": None, "return": 0.0},
                "worst_year": {"year": None, "return": 0.0},
                "best_month": {"month": None, "return": 0.0},
                "worst_month": {"month": None, "return": 0.0},
                "best_day": {"date": None, "return": 0.0},
                "worst_day": {"date": None, "return": 0.0}
            }

        # Calendar year returns aligned with _year_on_year_returns logic
        year_returns = self._calendar_year_return_tuples()

        # Calendar month returns using first and last NAV in each month
        month_buckets = {}
        for e in self.nav_data:
            key = (e['date'].year, e['date'].month)
            if key not in month_buckets:
                month_buckets[key] = {'start': e['nav'], 'end': e['nav']}
            else:
                month_buckets[key]['end'] = e['nav']

        month_returns = []
        for (y, m), vals in month_buckets.items():
            if vals['start'] != 0:
                ret = ((vals['end'] - vals['start']) / vals['start']) * 100
                month_returns.append((f"{y:04d}-{m:02d}", ret))

        # Daily returns from consecutive NAV observations
        day_returns = []
        for i in range(1, len(self.nav_data)):
            prev_nav = self.nav_data[i - 1]['nav']
            curr_nav = self.nav_data[i]['nav']
            if prev_nav != 0:
                ret = ((curr_nav - prev_nav) / prev_nav) * 100
                day_returns.append((self.nav_data[i]['date'].isoformat(), ret))

        max_pos_streak = 0
        max_neg_streak = 0
        curr_pos_streak = 0
        curr_neg_streak = 0
        for _, r in month_returns:
            if r > 0:
                curr_pos_streak += 1
                curr_neg_streak = 0
            elif r < 0:
                curr_neg_streak += 1
                curr_pos_streak = 0
            else:
                curr_pos_streak = 0
                curr_neg_streak = 0
            max_pos_streak = max(max_pos_streak, curr_pos_streak)
            max_neg_streak = max(max_neg_streak, curr_neg_streak)

        def pct_positive(items):
            if not items:
                return 0.0
            return round((sum(1 for _, r in items if r > 0) / len(items)) * 100, 2)

        def best_item(items, label_key):
            if not items:
                return {label_key: None, 'return': 0.0}
            k, v = max(items, key=lambda x: x[1])
            return {label_key: int(k) if label_key == 'year' else k, 'return': round(v, 2)}

        def worst_item(items, label_key):
            if not items:
                return {label_key: None, 'return': 0.0}
            k, v = min(items, key=lambda x: x[1])
            return {label_key: int(k) if label_key == 'year' else k, 'return': round(v, 2)}

        return {
            "positive_years_percent": pct_positive(year_returns),
            "positive_months_percent": pct_positive(month_returns),
            "positive_days_percent": pct_positive(day_returns),
            "max_consecutive_positive_months": max_pos_streak if month_returns else None,
            "max_consecutive_negative_months": max_neg_streak if month_returns else None,
            "best_year": best_item(year_returns, 'year'),
            "worst_year": worst_item(year_returns, 'year'),
            "best_month": best_item(month_returns, 'month'),
            "worst_month": worst_item(month_returns, 'month'),
            "best_day": best_item(day_returns, 'date'),
            "worst_day": worst_item(day_returns, 'date')
        }
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

                for i in monthly_indices:
                    start_date = dates[i]
                    start_nav = navs[i]
                    if start_nav <= 0:
                        continue
                    target_date = start_date + timedelta(days=window_days)

                    j = bisect.bisect_left(dates, target_date)
                    if j >= n:
                        break

                    end_date = dates[j]
                    end_nav = navs[j]
                    if end_nav <= 0:
                        continue

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
                            "median": round(sorted(rolling_values)[len(rolling_values) // 2] if len(rolling_values) % 2 == 1 else (sorted(rolling_values)[len(rolling_values) // 2 - 1] + sorted(rolling_values)[len(rolling_values) // 2]) / 2, 4),
                            "maximum": round(max(rolling_values), 4),
                            "minimum": round(min(rolling_values), 4),
                            "positive_percent": round(
                                (sum(1 for x in rolling_values if x > 0) / len(rolling_values)) * 100,
                                2
                            ),
                            "observations": len(rolling_values)
                        },
                        "points": rolling_points
                    }

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
            mdd_duration_details = {}
            annualized_volatility = {}
            sharpe_ratios = {}
            calmar_ratios = {}
            sortino_ratios = {}
            downside_deviation_values = {}
            skewness_values = {}
            kurtosis_values = {}
            pain_index_values = {}
            ulcer_index_values = {}
            sip_returns = {}

            for name, days in periods.items():
                past_nav, past_date = self._get_nav_for_period(days)
                absolute_returns[name] = self._absolute_return(past_nav)
                cagr_returns[name] = self._cagr(past_nav, past_date)
                mdd_info = self._mdd_duration_details(past_date)
                mdd_returns[name] = mdd_info["max_drawdown_percent"]
                mdd_duration_details[name] = mdd_info
                annualized_volatility[name] = self._annualized_volatility(past_date)
                sharpe_ratios[name] = self._sharpe_ratio(past_date)
                calmar_ratios[name] = self._calmar_ratio(cagr_returns[name], mdd_returns[name])
                sortino_ratios[name] = self._sortino_ratio(past_date)
                downside_deviation_values[name] = self._downside_deviation_percent(past_date)
                skewness_values[name] = self._skewness(past_date)
                kurtosis_values[name] = self._kurtosis(past_date)
                pain_index_values[name] = self._pain_index(past_date)
                ulcer_index_values[name] = self._ulcer_index(past_date)
                sip_returns[name] = self._sip_xirr(past_date, sip_amount=1000.0)

            ordered_absolute_periods = [
                ("one_day", 1),
                ("one_week", 7),
                ("one_month", 30),
                ("three_month", 90),
                ("six_month", 180),
                ("one_year", 365),
                ("two_year", 730),
                ("three_year", 1095),
                ("four_year", 1460),
                ("five_year", 1825),
                ("seven_year", 2555),
                ("ten_year", 3650),
            ]

            absolute_returns = {}
            for name, days in ordered_absolute_periods:
                past_nav, _ = self._get_nav_for_period(days)
                absolute_returns[name] = self._absolute_return(past_nav)

            launch_nav = self.nav_data[0]['nav']
            launch_date = self.nav_data[0]['date']

            absolute_returns["max"] = self._absolute_return(launch_nav)
            cagr_returns["max"] = self._cagr(launch_nav, launch_date)
            mdd_info_max = self._mdd_duration_details(launch_date)
            mdd_returns["max"] = mdd_info_max["max_drawdown_percent"]
            mdd_duration_details["max"] = mdd_info_max
            annualized_volatility["max"] = self._annualized_volatility(launch_date)
            sharpe_ratios["max"] = self._sharpe_ratio(launch_date)
            calmar_ratios["max"] = self._calmar_ratio(cagr_returns["max"], mdd_returns["max"])
            sortino_ratios["max"] = self._sortino_ratio(launch_date)
            downside_deviation_values["max"] = self._downside_deviation_percent(launch_date)
            skewness_values["max"] = self._skewness(launch_date)
            kurtosis_values["max"] = self._kurtosis(launch_date)
            pain_index_values["max"] = self._pain_index(launch_date)
            ulcer_index_values["max"] = self._ulcer_index(launch_date)
            sip_returns["max"] = self._sip_xirr(launch_date, sip_amount=1000.0)

            result = {
                "returns": {
                    "absolute_returns_percent": absolute_returns,
                    "cagr_percent": cagr_returns,
                    "year_on_year_percent": self._year_on_year_returns(),
                    "monthly_return_heatmap": self._monthly_return_heatmap(),
                    "return_distribution": self._return_distribution(),
                    "sip_returns": sip_returns,
                    "rolling_cagr_percent": self._rolling_cagr_all_periods(),
                },
                "risk_metrics": {
                    "volatility_annualized_percent": annualized_volatility,
                    "downside_deviation_percent": downside_deviation_values,
                    "skewness": skewness_values,
                    "kurtosis": kurtosis_values,
                },
                "drawdown": {
                    "current_drawdown": self._current_drawdown_details(),
                    "mdd_duration_details": mdd_duration_details,
                    "yearly_mdd_last_10_years": self._yearly_mdd_last_10_years(),
                    "drawdown_frequency": self._drawdown_frequency(),
                },
                "risk_adjusted_returns": {
                    "sharpe_ratio": sharpe_ratios,
                    "sortino_ratio": sortino_ratios,
                    "calmar_ratio": calmar_ratios,
                    "pain_index": pain_index_values,
                    "ulcer_index": ulcer_index_values,
                },
                "consistency": {
                    "consistency": self._consistency_metrics(),
                },
            }

            try:
                validated_result = NavMetricsOutput(**result)
                return json.loads(validated_result.model_dump_json(by_alias=True, exclude_none=True))
            except Exception as schema_error:
                logger.error(f"Output schema validation failed: {str(schema_error)}")
                raise

        except Exception as e:
            logger.error(f"Metric calculation failed: {str(e)}")
            raise


# metrics = NavMetrics(data[5]['data'])
# response_dict = metrics.get_all_metrics()
