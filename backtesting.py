from __future__ import annotations
from statistics import mean, median
from typing import Any, Dict, List
from historical_anomaly import analyze_candles


def _pct(a, b):
    try:
        if a in (None, 0) or b is None: return None
        return (float(b) / float(a) - 1.0) * 100.0
    except Exception:
        return None


def _forward_metrics(candles, i, horizon):
    if i >= len(candles): return None
    base = candles[i].get('close')
    end = i + horizon
    if base in (None, 0) or end >= len(candles):
        return {'status':'data-gap','bars_available':max(0, len(candles)-i-1)}
    closes=[c.get('close') for c in candles[i+1:end+1] if c.get('close') is not None]
    highs=[c.get('high') for c in candles[i+1:end+1] if c.get('high') is not None]
    lows=[c.get('low') for c in candles[i+1:end+1] if c.get('low') is not None]
    if not closes: return {'status':'data-gap','bars_available':0}
    ret=_pct(base, closes[-1])
    mfe=_pct(base, max(highs) if highs else max(closes))
    mae=_pct(base, min(lows) if lows else min(closes))
    return {'status':'complete','bars':len(closes),'end_timestamp':candles[end].get('timestamp'),'return_pct':round(ret,4) if ret is not None else None,'mfe_pct':round(mfe,4) if mfe is not None else None,'mae_pct':round(mae,4) if mae is not None else None}


def _summary(rows, horizon):
    vals=[r['forward'][str(horizon)]['return_pct'] for r in rows if r.get('forward',{}).get(str(horizon),{}).get('status')=='complete' and r['forward'][str(horizon)].get('return_pct') is not None]
    mfes=[r['forward'][str(horizon)]['mfe_pct'] for r in rows if r.get('forward',{}).get(str(horizon),{}).get('status')=='complete' and r['forward'][str(horizon)].get('mfe_pct') is not None]
    maes=[r['forward'][str(horizon)]['mae_pct'] for r in rows if r.get('forward',{}).get(str(horizon),{}).get('status')=='complete' and r['forward'][str(horizon)].get('mae_pct') is not None]
    return {'sample_count':len(vals),'positive_return_count':sum(v>0 for v in vals),'positive_return_rate_pct':round(sum(v>0 for v in vals)/len(vals)*100,2) if vals else None,'mean_return_pct':round(mean(vals),4) if vals else None,'median_return_pct':round(median(vals),4) if vals else None,'mean_mfe_pct':round(mean(mfes),4) if mfes else None,'mean_mae_pct':round(mean(maes),4) if maes else None}


def run_backtest(candles: List[dict], config: Dict[str,Any]|None=None):
    cfg=config or {}
    volume_window=max(5,int(cfg.get('volume_window',20)))
    return_window=max(5,int(cfg.get('return_window',20)))
    volume_threshold=float(cfg.get('volume_threshold',2.0))
    move_threshold=float(cfg.get('move_threshold',3.0))
    z_threshold=float(cfg.get('z_threshold',2.0))
    gap_threshold=float(cfg.get('gap_threshold',2.0))
    horizons=sorted(set(max(1,int(x)) for x in (cfg.get('horizons') or [1,3,5,10])))[:8]
    a=analyze_candles(candles,volume_window=volume_window,return_window=return_window)
    # Re-evaluate every bar with configurable thresholds; all features use prior bars only.
    events=[]
    for row in a.get('series',[]):
        flags=[]
        if row.get('volume_ratio') is not None and row['volume_ratio']>=volume_threshold: flags.append('volume_spike')
        if row.get('return_pct') is not None and abs(row['return_pct'])>=move_threshold: flags.append('large_move')
        if row.get('return_z') is not None and abs(row['return_z'])>=z_threshold: flags.append('return_zscore')
        if row.get('gap_pct') is not None and abs(row['gap_pct'])>=gap_threshold: flags.append('open_gap')
        if not flags: continue
        # locate the original row by timestamp; series is capped in analyze_candles, so use full input below.
        idx=next((j for j,c in enumerate(candles) if c.get('timestamp')==row.get('timestamp')),None)
        if idx is None: continue
        e={'timestamp':row.get('timestamp'),'index':idx,'price':row.get('close'),'rules':flags,'return_pct':row.get('return_pct'),'volume_ratio':row.get('volume_ratio'),'return_z':row.get('return_z'),'gap_pct':row.get('gap_pct'),'forward':{}}
        for h in horizons: e['forward'][str(h)]=_forward_metrics(candles,idx,h)
        events.append(e)
    summaries={str(h):_summary(events,h) for h in horizons}
    by_rule={}
    for rule in ['volume_spike','large_move','return_zscore','open_gap']:
        subset=[e for e in events if rule in e['rules']]
        by_rule[rule]={'event_count':len(subset),'horizons':{str(h):_summary(subset,h) for h in horizons}}
    complete_events=sum(1 for e in events if any(v.get('status')=='complete' for v in e['forward'].values()))
    return {'status':'complete' if candles else 'data-gap','candles_analyzed':len(candles),'event_count':len(events),'events_with_any_complete_forward':complete_events,'horizons':horizons,'config':{'volume_window':volume_window,'return_window':return_window,'volume_threshold':volume_threshold,'move_threshold':move_threshold,'z_threshold':z_threshold,'gap_threshold':gap_threshold},'summary':summaries,'by_rule':by_rule,'events':events[-300:],'method':'historical validation only; event features use data available at the event; forward metrics use subsequent bars; no buy/sell inference'}
