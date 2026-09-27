
from typing import Any, Dict, List

def _status(item):
    if not isinstance(item, dict): return "UNKNOWN"
    if item.get("status") == "success": return "OK"
    if item.get("data"): return "OK"
    if item.get("error"): return "ERROR"
    return "GAP"

def summarize(symbol: str, research: Dict[str, Any]) -> Dict[str, Any]:
    analysis = research.get("analysis") or {}
    flags = analysis.get("flags") or []
    gaps = analysis.get("data_gaps") or []
    stages = {}
    for k in ["quote","news","income_statement","balance_sheet","cash_flow","share_holdings","key_ratios","corporate_actions"]:
        if k in research:
            stages[k] = _status(research[k])
    return {
        "symbol": symbol.upper(),
        "flags_count": len(flags),
        "gap_count": len(gaps),
        "flags": [x.get("item", str(x)) if isinstance(x,dict) else str(x) for x in flags],
        "gaps": [str(x) for x in gaps],
        "stages": stages,
        "freshness": research.get("freshness") or research.get("as_of") or None,
    }

def scan(symbols: List[str], research_fn) -> Dict[str, Any]:
    rows=[]
    for s in symbols[:20]:
        s=s.strip().upper()
        if not s: continue
        try:
            r=research_fn(s)
            rows.append(summarize(s,r))
        except Exception as e:
            rows.append({"symbol":s,"flags_count":0,"gap_count":0,"flags":[],"gaps":[str(e)],"stages":{},"freshness":None})
    return {
        "status":"success",
        "count":len(rows),
        "rows":rows,
        "note":"This dashboard is descriptive. It does not rank stocks or issue buy/sell recommendations."
    }
