from __future__ import annotations
from datetime import datetime, timedelta, timezone
from statistics import mean, pstdev
from typing import Any, Dict, List
import math


def _num(v):
    try:
        if v is None: return None
        return float(v)
    except Exception:
        return None


def normalize_candles(candles: List[list]) -> List[dict]:
    out=[]
    for c in candles or []:
        if not isinstance(c,list) or len(c)<6: continue
        out.append({'timestamp':c[0],'open':_num(c[1]),'high':_num(c[2]),'low':_num(c[3]),'close':_num(c[4]),'volume':_num(c[5]),'open_interest':_num(c[6]) if len(c)>6 else None})
    return list(reversed(out))


def _z(value, sample):
    vals=[x for x in sample if x is not None]
    if len(vals)<5: return None
    sd=pstdev(vals)
    if sd==0: return 0.0
    return (value-mean(vals))/sd


def analyze_candles(candles: List[dict], volume_window=20, return_window=20) -> Dict[str,Any]:
    rows=[]; anomalies=[]
    for i,c in enumerate(candles):
        close=c.get('close'); prev=candles[i-1].get('close') if i else None
        ret=((close/prev)-1)*100 if close is not None and prev not in (None,0) else None
        prior_vol=[x.get('volume') for x in candles[max(0,i-volume_window):i] if x.get('volume') is not None]
        prior_ret=[]
        for j in range(max(1,i-return_window),i):
            a=candles[j-1].get('close'); b=candles[j].get('close')
            if a not in (None,0) and b is not None: prior_ret.append((b/a-1)*100)
        vol_avg=mean(prior_vol) if prior_vol else None
        vol_ratio=(c.get('volume')/vol_avg) if vol_avg not in (None,0) and c.get('volume') is not None else None
        ret_z=_z(ret,prior_ret) if ret is not None else None
        gap=((c.get('open')/prev)-1)*100 if c.get('open') is not None and prev not in (None,0) else None
        range_pct=((c.get('high')-c.get('low'))/prev*100) if None not in (c.get('high'),c.get('low'),prev) and prev!=0 else None
        flags=[]
        if vol_ratio is not None and vol_ratio>=2: flags.append('volume >= 2x prior average')
        if ret is not None and abs(ret)>=3: flags.append('daily move >= 3%')
        if ret_z is not None and abs(ret_z)>=2: flags.append('return z-score >= 2')
        if gap is not None and abs(gap)>=2: flags.append('open gap >= 2%')
        row={**c,'return_pct':round(ret,4) if ret is not None else None,'volume_avg':round(vol_avg,2) if vol_avg is not None else None,'volume_ratio':round(vol_ratio,3) if vol_ratio is not None else None,'return_z':round(ret_z,3) if ret_z is not None else None,'gap_pct':round(gap,4) if gap is not None else None,'range_pct':round(range_pct,4) if range_pct is not None else None,'flags':flags}
        rows.append(row)
        if flags: anomalies.append(row)
    return {'status':'success','candles_analyzed':len(rows),'anomaly_count':len(anomalies),'anomalies':anomalies[-50:],'series':rows[-250:],'method':'rule-based anomaly detection; thresholds are descriptive screening flags, not buy/sell signals'}


def _date(s):
    try:
        return datetime.fromisoformat(s.replace('Z','+00:00')).date()
    except Exception:
        try: return datetime.strptime(s[:10],'%Y-%m-%d').date()
        except Exception: return None


def match_news_to_anomalies(anomalies: List[dict], news_items: List[dict], days_before=1, days_after=1) -> Dict[str,Any]:
    out=[]
    for a in anomalies or []:
        ad=_date(str(a.get('timestamp','')))
        if not ad: continue
        matches=[]
        for n in news_items or []:
            nd=_date(str(n.get('published','')))
            if not nd: continue
            delta=(nd-ad).days
            if -days_before <= delta <= days_after:
                matches.append({'title':n.get('title'),'published':n.get('published'),'source':n.get('source'),'link':n.get('link'),'day_offset':delta})
        out.append({'timestamp':a.get('timestamp'),'return_pct':a.get('return_pct'),'volume_ratio':a.get('volume_ratio'),'flags':a.get('flags',[]),'news_matches':matches[:20],'news_match_count':len(matches)})
    return {'status':'success','anomaly_events':len(out),'events_with_news':sum(1 for x in out if x['news_match_count']),'matches':out,'window_days':{'before':days_before,'after':days_after},'method':'date-window association only; correlation/causation is not inferred'}


def build_intraday_timeline(candles: List[dict], news_items: List[dict], volume_window=30, return_window=30, news_window_hours=24) -> Dict[str,Any]:
    """Intraday anomaly scan + event timeline. Associations are temporal only; no causation inferred."""
    a=analyze_candles(candles, volume_window=volume_window, return_window=return_window)
    events=[]
    for x in a.get('anomalies',[]):
        events.append({'type':'price_volume_anomaly','timestamp':x.get('timestamp'),'return_pct':x.get('return_pct'),'volume_ratio':x.get('volume_ratio'),'flags':x.get('flags',[]),'news':[]})
    def dt(v):
        try:
            s=str(v).replace('Z','+00:00')
            d=datetime.fromisoformat(s)
            if d.tzinfo is None: d=d.replace(tzinfo=timezone.utc)
            return d
        except Exception: return None
    news_norm=[]
    for n in news_items or []:
        d=dt(n.get('published'))
        if d: news_norm.append((d,n))
    for e in events:
        ed=dt(e.get('timestamp'))
        if not ed: continue
        for nd,n in news_norm:
            hours=abs((nd-ed).total_seconds())/3600
            if hours<=news_window_hours:
                e['news'].append({'title':n.get('title'),'published':n.get('published'),'source':n.get('source'),'link':n.get('link'),'hours_from_event':round((nd-ed).total_seconds()/3600,2)})
        e['news']=e['news'][:20]
    timeline=[]
    for e in events: timeline.append(e)
    for nd,n in news_norm:
        timeline.append({'type':'news','timestamp':n.get('published'),'title':n.get('title'),'source':n.get('source'),'link':n.get('link')})
    timeline.sort(key=lambda x: str(x.get('timestamp','')), reverse=True)
    return {'status':'success','candles_analyzed':a.get('candles_analyzed',0),'anomaly_count':a.get('anomaly_count',0),'events_with_news':sum(1 for e in events if e.get('news')),'events':events[-100:],'timeline':timeline[:200],'method':'intraday rule-based anomaly detection; temporal news association only; no correlation/causation or buy/sell inference'}
