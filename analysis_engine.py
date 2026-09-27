
from typing import Any, Dict, List

def _num(x):
    if x is None: return None
    try:
        return float(str(x).replace(",", "").replace("%","").strip())
    except Exception:
        return None

def _history(data, category):
    for item in (data or []):
        if str(item.get("category","")).lower() == category.lower():
            return item.get("history") or []
    return []

def pct_change(old, new):
    old, new = _num(old), _num(new)
    if old in (None, 0) or new is None: return None
    return (new-old)/abs(old)*100

def build_analysis(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = {"flags": [], "comparisons": [], "data_gaps": [], "observations": []}
    inc = payload.get("income_statement") or {}
    cash = payload.get("cash_flow") or {}
    bs = payload.get("balance_sheet") or {}
    sh = payload.get("share_holdings") or {}
    ratios = payload.get("key_ratios") or {}

    # Trend checks
    rev = _history(inc.get("income_statement"), "revenue")
    profit = _history(inc.get("income_statement"), "net profit")
    if len(rev) >= 2:
        ch = pct_change(rev[-2].get("value"), rev[-1].get("value"))
        out["observations"].append({"metric":"Revenue latest change","value":ch,"unit":"%"})
        if ch is not None and ch < -10:
            out["flags"].append({"type":"watch","item":"Revenue fell >10% in latest reported period"})
    else:
        out["data_gaps"].append("Revenue history is insufficient for a trend check.")

    if len(profit) >= 2:
        ch = pct_change(profit[-2].get("value"), profit[-1].get("value"))
        out["observations"].append({"metric":"Net profit latest change","value":ch,"unit":"%"})
        if ch is not None and ch < -15:
            out["flags"].append({"type":"watch","item":"Net profit fell >15% in latest reported period"})
    else:
        out["data_gaps"].append("Net profit history is insufficient for a trend check.")

    # Cash flow quality
    opcf = _history(cash.get("cash_flow"), "operating")
    if opcf and profit:
        c = _num(opcf[-1].get("value")); p = _num(profit[-1].get("value"))
        if c is not None and p is not None:
            out["observations"].append({"metric":"Operating cash flow / net profit","value":(c/p if p else None),"unit":"x"})
            if p > 0 and c < 0:
                out["flags"].append({"type":"watch","item":"Latest net profit is positive while operating cash flow is negative"})
    else:
        out["data_gaps"].append("Operating cash flow or profit history unavailable.")

    # Promoter trend
    prom = _history(sh.get("share_holdings"), "promoters")
    if len(prom) >= 2:
        delta = _num(prom[-1].get("value")) - _num(prom[-2].get("value"))
        out["observations"].append({"metric":"Promoter holding change","value":delta,"unit":"percentage points"})
        if delta < -1:
            out["flags"].append({"type":"watch","item":"Promoter holding decreased by more than 1 percentage point in latest quarter"})
    else:
        out["data_gaps"].append("Promoter shareholding history is insufficient.")

    # Sector-relative ratios
    if isinstance(ratios, list):
        for r in ratios:
            cv, sv = _num(r.get("company_value")), _num(r.get("sector_value"))
            if cv is not None and sv is not None:
                out["comparisons"].append({
                    "ratio": r.get("name"),
                    "company": cv,
                    "sector": sv,
                    "difference": cv-sv
                })
    elif isinstance(ratios, dict):
        rows = ratios.get("data") or []
        for r in rows:
            cv, sv = _num(r.get("company_value")), _num(r.get("sector_value"))
            if cv is not None and sv is not None:
                out["comparisons"].append({"ratio":r.get("name"),"company":cv,"sector":sv,"difference":cv-sv})
    if not out["comparisons"]:
        out["data_gaps"].append("Sector-relative ratio data unavailable.")

    return out
