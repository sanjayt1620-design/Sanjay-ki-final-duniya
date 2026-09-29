from multi_stock_scanner import scan as scan_stocks
from analysis_engine import build_analysis
from historical_anomaly import normalize_candles, analyze_candles, match_news_to_anomalies, build_intraday_timeline
from backtesting import run_backtest
import os, datetime, requests, xml.etree.ElementTree as ET, sqlite3, json, hashlib, threading
from datetime import timedelta
from urllib.parse import quote as urlquote
from flask import Flask, jsonify, request
from flask_cors import CORS

app=Flask(__name__, static_folder='web', static_url_path='')
cors_origins=os.getenv('CORS_ORIGINS','').strip()
if cors_origins:
    CORS(app, origins=[x.strip() for x in cors_origins.split(',') if x.strip()])
UPSTOX_BASE=os.getenv('UPSTOX_API_BASE','https://api.upstox.com').rstrip('/')
PORT=int(os.getenv('PORT','8787'))
CACHE_TTL=int(os.getenv('CACHE_TTL_SECONDS','30'))
_cache={}
_cache_lock=threading.RLock()
_db_lock=threading.RLock()
DB_PATH=os.getenv('RESEARCH_DB_PATH', os.path.join(os.path.dirname(__file__), 'research_history.sqlite3'))

def db_connect():
    con=sqlite3.connect(DB_PATH, timeout=15, check_same_thread=False)
    con.execute('PRAGMA busy_timeout=15000')
    try: con.execute('PRAGMA journal_mode=WAL')
    except sqlite3.DatabaseError: pass
    con.execute('PRAGMA synchronous=NORMAL')
    return con

def db_init():
    con=db_connect()
    con.execute("CREATE TABLE IF NOT EXISTS research_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, generated_at TEXT, symbols TEXT, payload TEXT)")
    con.execute("CREATE TABLE IF NOT EXISTS research_events (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, symbol TEXT, event_type TEXT, timestamp TEXT, move_pct REAL, volume_ratio REAL, flags TEXT, news_count INTEGER, payload TEXT)")
    con.execute("CREATE TABLE IF NOT EXISTS evidence_ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, finding_type TEXT, summary TEXT, source_name TEXT, source_url TEXT, source_type TEXT, observed_at TEXT, published_at TEXT, data_asof TEXT, fingerprint TEXT, status TEXT)")
    con.execute("CREATE TABLE IF NOT EXISTS research_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, generated_at TEXT, fingerprint TEXT, summary TEXT, payload TEXT)")
    con.execute("CREATE TABLE IF NOT EXISTS contradictions (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, topic TEXT, left_summary TEXT, left_source TEXT, right_summary TEXT, right_source TEXT, detected_at TEXT)")
    con.execute("CREATE TABLE IF NOT EXISTS monitoring_alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, fingerprint TEXT, alert_type TEXT, summary TEXT, created_at TEXT, payload TEXT)")
    con.execute("CREATE TABLE IF NOT EXISTS saved_moments (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, saved_at TEXT, title TEXT, trigger TEXT, summary TEXT, payload TEXT)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_research_runs_generated ON research_runs(generated_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_research_events_symbol_id ON research_events(symbol,id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_evidence_symbol_id ON evidence_ledger(symbol,id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_symbol_id ON research_snapshots(symbol,id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_alerts_symbol_id ON monitoring_alerts(symbol,id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_moments_symbol_id ON saved_moments(symbol,id)")
    con.commit(); con.close()

def db_save_run(payload):
    with _db_lock:
        db_init(); con=db_connect()
        cur=con.execute("INSERT INTO research_runs(generated_at,symbols,payload) VALUES(?,?,?)", (payload.get('generated_at'), ','.join(x.get('symbol','') for x in payload.get('rows',[])), json.dumps(payload)))
        rid=cur.lastrowid
        for row in payload.get('rows',[]):
            i=row.get('intraday') or {}; latest=i.get('latest') or {}
            if latest:
                con.execute("INSERT INTO research_events(run_id,symbol,event_type,timestamp,move_pct,volume_ratio,flags,news_count,payload) VALUES(?,?,?,?,?,?,?,?,?)",(rid,row.get('symbol'),'price_volume_anomaly',latest.get('timestamp'),latest.get('return_pct'),latest.get('volume_ratio'),json.dumps(latest.get('flags',[])),i.get('events_with_news',0),json.dumps(latest)))
        con.commit(); con.close(); return rid


db_init()


def source_type(url):
    u=(url or '').lower()
    if any(x in u for x in ('nseindia.com','bseindia.com','sebi.gov.in')): return 'exchange/regulator'
    if 'upstox.com' in u: return 'data/broker'
    if 'news.google.com' in u: return 'news_aggregator'
    return 'news/media/other'

def evidence_fingerprint(summary, source_url=''):
    return hashlib.sha256((str(summary)+'|'+str(source_url)).encode()).hexdigest()[:24]

def record_evidence(symbol, finding_type, summary, source_name='', source_url='', published_at=None, data_asof=None, status='observed'):
    db_init(); con=db_connect(); fp=evidence_fingerprint(summary,source_url)
    con.execute("INSERT INTO evidence_ledger(symbol,finding_type,summary,source_name,source_url,source_type,observed_at,published_at,data_asof,fingerprint,status) VALUES(?,?,?,?,?,?,?,?,?,?,?)",(symbol,finding_type,summary,source_name,source_url,source_type(source_url),now(),published_at,data_asof,fp,status)); con.commit(); con.close(); return fp

def snapshot_diff(symbol, payload):
    db_init(); con=db_connect(); con.row_factory=sqlite3.Row
    compact=json.dumps({'quote':payload.get('quote'), 'risk':payload.get('risk_engine'), 'news_count':len((payload.get('news') or {}).get('items') or [])},sort_keys=True,default=str)
    fp=hashlib.sha256(compact.encode()).hexdigest()
    prev=con.execute("SELECT fingerprint,summary,generated_at FROM research_snapshots WHERE symbol=? ORDER BY id DESC LIMIT 1",(symbol,)).fetchone()
    changed=not prev or prev['fingerprint']!=fp
    summary='Changed since previous snapshot' if changed and prev else ('First snapshot' if not prev else 'No material snapshot change')
    con.execute("INSERT INTO research_snapshots(symbol,generated_at,fingerprint,summary,payload) VALUES(?,?,?,?,?)",(symbol,now(),fp,summary,json.dumps(payload,default=str))); con.commit(); con.close()
    return {'changed':changed,'summary':summary,'previous_generated_at':prev['generated_at'] if prev else None}

def build_v17_evidence(symbol, payload):
    q=payload.get('quote') or {}; n=payload.get('news') or {}
    if q.get('last_price') is not None: record_evidence(symbol,'quote',f"LTP {q.get('last_price')}",'Upstox',UPSTOX_BASE,data_asof=q.get('last_trade_time'))
    for item in (n.get('items') or [])[:20]: record_evidence(symbol,'news',item.get('title') or '',item.get('source') or 'Google News RSS',item.get('link') or '',item.get('published'))


def now(): return datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()
def api_json(url,params=None,headers=None):
    r=requests.get(url,params=params,headers=headers or {},timeout=20); 
    try: data=r.json()
    except Exception: data={'raw':r.text[:2000]}
    if r.status_code>=400: raise RuntimeError(f'HTTP {r.status_code}: {data}')
    return data

def cached(key,fn):
    t=datetime.datetime.now(datetime.timezone.utc).timestamp(); hit=_cache.get(key)
    if hit and t-hit[0]<CACHE_TTL: return hit[1]
    val=fn(); _cache[key]=(t,val); return val

def up_headers():
    token=os.getenv('UPSTOX_ACCESS_TOKEN','').strip()
    if not token: raise RuntimeError('UPSTOX_ACCESS_TOKEN missing')
    return {'Accept':'application/json','Authorization':f'Bearer {token}'}

def find_nse_equity(symbol):
    def f():
        data=api_json(f'{UPSTOX_BASE}/v2/instruments/search',params={'query':symbol,'exchanges':'NSE','segments':'EQ','page_number':1,'records':20},headers=up_headers())
        rows=data.get('data') or []; su=symbol.upper()
        exact=[x for x in rows if str(x.get('trading_symbol','')).upper()==su and x.get('segment')=='NSE_EQ']
        if exact:return exact[0]
        eq=[x for x in rows if x.get('segment')=='NSE_EQ']; return eq[0] if eq else None
    return cached('inst:'+symbol,f)

def upstox_quote(symbol):
    inst=find_nse_equity(symbol)
    if not inst:return {'status':'not_found','symbol':symbol,'provider':'Upstox'}
    key=inst['instrument_key']
    def f():
        data=api_json(f'{UPSTOX_BASE}/v3/market-quote/quotes',params={'instrument_key':key},headers=up_headers())
        raw=(data.get('data') or {}).get(key) or {}; ltpc=raw.get('ltpc') or {}; ohlc=raw.get('ohlc') or {}
        return {'status':'ok','provider':'Upstox','exchange':'NSE','instrument_key':key,'trading_symbol':inst.get('trading_symbol'),'name':inst.get('name'),'isin':inst.get('isin'),'last_price':ltpc.get('ltp'),'previous_close':ltpc.get('cp'),'last_trade_time':ltpc.get('ltt'),'last_trade_qty':ltpc.get('ltq'),'open':(ohlc.get('open') or {}).get('price'),'high':(ohlc.get('high') or {}).get('price'),'low':(ohlc.get('low') or {}).get('price'),'volume':(ohlc.get('volume') or raw.get('volume')),'year_high':raw.get('year_high'),'year_low':raw.get('year_low')}
    return cached('quote:'+key,f)

def upstox_intraday_depth(symbol):
    """Live intraday market-depth snapshot. Buy/sell quantities are outstanding bid/ask quantities, not guaranteed executed trade-side volume."""
    inst=find_nse_equity(symbol)
    if not inst:return {'status':'not_found','symbol':symbol,'provider':'Upstox'}
    key=inst['instrument_key']
    try:
        data=api_json(f'{UPSTOX_BASE}/v3/market-quote/quotes',params={'instrument_key':key},headers=up_headers())
        raw=(data.get('data') or {}).get(key) or {}
        ltpc=raw.get('ltpc') or {}
        depth=raw.get('depth') or raw.get('market_depth') or {}
        buy=depth.get('buy') or raw.get('buy') or []
        sell=depth.get('sell') or raw.get('sell') or []
        def levels(rows):
            out=[]
            for x in rows[:5] if isinstance(rows,list) else []:
                out.append({'price':x.get('price'),'quantity':x.get('quantity'),'orders':x.get('orders') or x.get('order_count')})
            return out
        buy5=levels(buy); sell5=levels(sell)
        total_buy=raw.get('total_buy_quantity')
        total_sell=raw.get('total_sell_quantity')
        if total_buy is None: total_buy=sum((x.get('quantity') or 0) for x in buy5 if isinstance(x.get('quantity'),(int,float)))
        if total_sell is None: total_sell=sum((x.get('quantity') or 0) for x in sell5 if isinstance(x.get('quantity'),(int,float)))
        return {'status':'ok','provider':'Upstox','exchange':'NSE','symbol':symbol,'instrument_key':key,'trading_symbol':inst.get('trading_symbol'),'name':inst.get('name'),'last_price':ltpc.get('ltp'),'last_trade_qty':ltpc.get('ltq'),'last_trade_time':ltpc.get('ltt'),'volume':raw.get('volume'),'total_buy_quantity':total_buy,'total_sell_quantity':total_sell,'buy_levels':buy5,'sell_levels':sell5,'fetched_at':now(),'note':'Buy/sell quantities represent visible market-depth bid/ask quantities where supplied by the provider; they are not a definitive count of executed buyer-vs-seller trades.'}
    except Exception as e:
        return {'status':'error','symbol':symbol,'provider':'Upstox','error':str(e),'note':'Provider may not expose market-depth totals for this instrument/account.'}

def google_news(symbol):
    """Multi-query Google News RSS discovery with deduplication. It is still news discovery, not source verification."""
    def f():
        queries=[f'{symbol} NSE India stock', f'{symbol} results earnings', f'{symbol} order acquisition merger', f'{symbol} SEBI NSE BSE filing']
        items=[]; seen=set()
        for query in queries:
            try:
                q=urlquote(query); url=f'https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en'
                r=requests.get(url,timeout=20,headers={'User-Agent':'PersonalStockResearch/5.0'}); r.raise_for_status(); root=ET.fromstring(r.content)
                for item in root.findall('./channel/item')[:20]:
                    title=(item.findtext('title') or '').strip(); link=(item.findtext('link') or '').strip()
                    key=(title.lower(),link)
                    if not title or key in seen: continue
                    seen.add(key); items.append({'title':title,'link':link,'published':item.findtext('pubDate'),'source':(item.find('source').text if item.find('source') is not None else None),'query':query})
            except Exception as e:
                continue
        items.sort(key=lambda x:x.get('published') or '', reverse=True)
        return {'status':'ok' if items else 'gap','provider':'Google News RSS multi-query discovery','queries':queries,'items':items[:60]}
    return cached('news_multi:'+symbol,f)


def risk_engine(q,news):
    flags=[]; facts=[]
    if q.get('status')=='ok':
        lp,pc=q.get('last_price'),q.get('previous_close')
        if isinstance(lp,(int,float)) and isinstance(pc,(int,float)) and pc:
            ch=(lp/pc-1)*100; facts.append({'check':'1-day price change','value':round(ch,2),'unit':'%'})
            if abs(ch)>=5: flags.append({'level':'attention','check':'large 1-day move','evidence':f'{ch:.2f}% vs previous close'})
        vol=q.get('volume')
        if vol is not None: facts.append({'check':'volume available','value':vol})
        yh,yl=q.get('year_high'),q.get('year_low')
        if yh and yl and q.get('last_price'):
            facts.append({'check':'52-week range','value':f'{yl} - {yh}'})
    items=(news or {}).get('items') or []
    facts.append({'check':'news items retrieved','value':len(items)})
    return {'status':'ok','method':'rule-based evidence flags only','flags':flags,'facts':facts,'not_checked':['debt','promoter pledge','results trend','cash flow','auditor issues','legal/regulatory risk','valuation']}



def up_fundamental(isin, endpoint, params=None):
    if not isin: return {'status':'gap','reason':'ISIN not available'}
    def f():
        return api_json(f'{UPSTOX_BASE}/v2/fundamentals/{isin}/{endpoint}', params=params or {}, headers=up_headers())
    return cached(f'fund:{isin}:{endpoint}:{str(params)}', f)

def fundamentals_bundle(isin):
    if not isin: return {'status':'gap','reason':'ISIN not available'}
    out={}
    specs={
      'income_statement':('income-statement',{'type':'consolidated','time_period':'quarterly','fs':'true'}),
      'income_yearly':('income-statement',{'type':'consolidated','time_period':'yearly','fs':'true'}),
      'balance_sheet':('balance-sheet',{'type':'consolidated','fs':'true'}),
      'cash_flow':('cash-flow',{'type':'consolidated','fs':'true'}),
      'key_ratios':('key-ratios',{}),
      'share_holdings':('share-holdings',{}),
      'corporate_actions':('corporate-actions',{}),
    }
    successes=0; failures=[]
    for name,(ep,params) in specs.items():
        try:
            out[name]=up_fundamental(isin,ep,params)
            if (out[name] or {}).get('status')=='success': successes+=1
            else: failures.append(name)
        except Exception as e:
            out[name]={'status':'error','error':str(e)}; failures.append(name)
    out['status']='ok' if successes==len(specs) else ('partial' if successes else 'gap')
    out['successful_endpoints']=successes
    out['failed_endpoints']=failures
    out['provider']='Upstox Company Fundamentals API'
    out['fetched_at']=now()
    return out

def extract_latest_history(bundle, category, container_key):
    data=((bundle.get(container_key) or {}).get('data') or {}).get(container_key) or []
    for row in data:
        if row.get('category')==category and row.get('history'):
            return row['history'][0]
    return None

def fundamental_risk(bundle):
    flags=[]; facts=[]
    inc=bundle.get('income_statement',{}).get('data',{}).get('income_statement',[]) if bundle.get('income_statement',{}).get('status')=='success' else []
    rev=next((x for x in inc if x.get('category')=='revenue'),None)
    profit=next((x for x in inc if x.get('category')=='net_profit'),None)
    if rev and rev.get('history'):
        h=rev['history'][0]; facts.append({'check':'latest revenue','value':h.get('value'),'period':h.get('period'),'unit':'crore'})
        if isinstance(h.get('change'),str) and h['change'].startswith('-'):
            flags.append({'level':'attention','check':'revenue decline','evidence':h['change']})
    if profit and profit.get('history'):
        h=profit['history'][0]; facts.append({'check':'latest net profit','value':h.get('value'),'period':h.get('period'),'unit':'crore'})
        if isinstance(h.get('change'),str) and h['change'].startswith('-'):
            flags.append({'level':'attention','check':'net profit decline','evidence':h['change']})
    bs=bundle.get('balance_sheet',{}).get('data',{}) if bundle.get('balance_sheet',{}).get('status')=='success' else {}
    hist=bs.get('history') or []
    if hist: facts.append({'check':'latest total liabilities','value':hist[0].get('total_liability'),'period':hist[0].get('period'),'unit':'crore'})
    cf=bundle.get('cash_flow',{}).get('data',{}) if bundle.get('cash_flow',{}).get('status')=='success' else {}
    cfs=cf.get('cash_flow') or []
    op=next((x for x in cfs if x.get('category')=='operating'),None)
    if op and op.get('history'):
        h=op['history'][0]; facts.append({'check':'latest operating cash flow','value':h.get('value'),'period':h.get('period'),'unit':'crore'})
        if isinstance(h.get('value'),(int,float)) and h['value']<0: flags.append({'level':'attention','check':'negative operating cash flow','evidence':str(h.get('value'))+' crore'})
    ratios=bundle.get('key_ratios',{}).get('data') if bundle.get('key_ratios',{}).get('status')=='success' else []
    for r in ratios or []:
        if r.get('name') in ('ROE','ROCE','P/E','P/B','EV/EBITDA'): facts.append({'check':r.get('name'),'value':r.get('company_value'),'sector':r.get('sector_value')})
    sh=bundle.get('share_holdings',{}).get('data') if bundle.get('share_holdings',{}).get('status')=='success' else []
    prom=next((x for x in sh or [] if x.get('category')=='promoters'),None)
    if prom and prom.get('history'):
        h=prom['history'][0]; facts.append({'check':'promoter holding','value':h.get('value'),'period':h.get('period'),'unit':'%'})
    return {'flags':flags,'facts':facts,'method':'rule-based evidence flags; not a buy/sell recommendation'}




def pct_change(a,b):
    try:
        if b in (None,0) or a is None: return None
        return (a/b-1)*100
    except Exception: return None

def history_values(bundle, container, category):
    block=bundle.get(container,{})
    if block.get('status')!='success': return []
    data=(block.get('data') or {}).get(container) or []
    row=next((x for x in data if x.get('category')==category),None)
    return (row or {}).get('history') or []

def analyst_report(symbol, quote, news, bundle):
    sections=[]; gaps=[]; observations=[]
    if quote.get('status')=='ok':
        lp,pc=quote.get('last_price'),quote.get('previous_close')
        if isinstance(lp,(int,float)) and isinstance(pc,(int,float)) and pc:
            observations.append(f"Price is {lp:.2f}; day change is {((lp/pc)-1)*100:.2f}% versus previous close.")
    else: gaps.append('live quote')

    rev=history_values(bundle,'income_statement','revenue')
    np=history_values(bundle,'income_statement','net_profit')
    op=history_values(bundle,'income_statement','operating_profit')
    if rev:
        observations.append(f"Latest revenue: {rev[0].get('value')} crore ({rev[0].get('period')}).")
        if len(rev)>1:
            c=pct_change(rev[0].get('value'),rev[1].get('value'))
            if c is not None: observations.append(f"Revenue year/period comparison in returned series: {c:.2f}%.")
    else: gaps.append('revenue trend')
    if np:
        observations.append(f"Latest net profit: {np[0].get('value')} crore ({np[0].get('period')}).")
        if len(np)>1:
            c=pct_change(np[0].get('value'),np[1].get('value'))
            if c is not None: observations.append(f"Net profit comparison in returned series: {c:.2f}%.")
    else: gaps.append('net profit trend')
    if op: observations.append(f"Latest operating profit: {op[0].get('value')} crore ({op[0].get('period')}).")

    cf=history_values(bundle,'cash_flow','operating')
    if cf:
        observations.append(f"Latest operating cash flow: {cf[0].get('value')} crore ({cf[0].get('period')}).")
        if isinstance(cf[0].get('value'),(int,float)) and cf[0]['value'] < 0:
            sections.append({'type':'attention','title':'Cash-flow check','text':'Latest operating cash flow is negative in the returned data.'})
    else: gaps.append('operating cash flow')

    bs=bundle.get('balance_sheet',{}).get('data',{}) if bundle.get('balance_sheet',{}).get('status')=='success' else {}
    hist=bs.get('history') or []
    if hist:
        observations.append(f"Latest total liabilities: {hist[0].get('total_liability')} crore; total assets: {hist[0].get('total_asset')} crore ({hist[0].get('period')}).")
        if len(hist)>1:
            c=pct_change(hist[0].get('total_liability'),hist[1].get('total_liability'))
            if c is not None: observations.append(f"Total liabilities changed {c:.2f}% versus the prior returned period.")
    else: gaps.append('balance-sheet liabilities')

    sh=bundle.get('share_holdings',{}).get('data') if bundle.get('share_holdings',{}).get('status')=='success' else []
    prom=next((x for x in sh or [] if x.get('category')=='promoters'),None)
    if prom and prom.get('history'):
        h=prom['history']; observations.append(f"Latest promoter holding: {h[0].get('value')}% ({h[0].get('period')}).")
        if len(h)>1:
            delta=(h[0].get('value') or 0)-(h[1].get('value') or 0)
            observations.append(f"Promoter holding changed {delta:+.2f} percentage points versus the prior returned quarter.")
    else: gaps.append('promoter/shareholding trend')

    ratios=bundle.get('key_ratios',{}).get('data') if bundle.get('key_ratios',{}).get('status')=='success' else []
    if ratios:
        for r in ratios:
            observations.append(f"{r.get('name')}: company {r.get('company_value')}, sector {r.get('sector_value')}.")
    else: gaps.append('valuation/return ratios')

    actions=bundle.get('corporate_actions',{}).get('data') if bundle.get('corporate_actions',{}).get('status')=='success' else []
    if actions: observations.append(f"Corporate actions returned: {len(actions)} event(s).")
    else: gaps.append('corporate actions')
    if not news.get('items'): gaps.append('news')
    sections.append({'type':'neutral','title':'What the engine found','text':' '.join(observations) if observations else 'No sufficient data returned yet.'})
    sections.append({'type':'gap','title':'Still needs verification','text':', '.join(gaps) if gaps else 'No configured research-stage gaps.'})
    return {'symbol':symbol,'sections':sections,'gaps':gaps,'observations':observations,'method':'descriptive evidence summary; not investment advice'}


def parse_num(v):
    if isinstance(v,(int,float)): return float(v)
    if isinstance(v,str):
        try: return float(v.replace(',','').replace('%','').strip())
        except: return None
    return None

def trend_check(history, label, unit='crore'):
    out=[]
    if not history: return out
    vals=[parse_num(x.get('value')) for x in history[:4]]
    for i,v in enumerate(vals):
        if v is None: continue
        out.append({'period':history[i].get('period'),'value':v,'unit':unit})
    return out

def deep_analysis(symbol, bundle, news):
    findings=[]; flags=[]; gaps=[]
    rev=history_values(bundle,'income_statement','revenue')
    profit=history_values(bundle,'income_statement','net_profit')
    op=history_values(bundle,'income_statement','operating_profit')
    ocf=history_values(bundle,'cash_flow','operating')
    if len(rev)>=2:
        c=pct_change(parse_num(rev[0].get('value')),parse_num(rev[1].get('value')))
        if c is not None: findings.append({'topic':'Revenue trend','latest':rev[0],'change_vs_previous_pct':round(c,2)})
        if c is not None and c<0: flags.append({'level':'attention','check':'revenue declined','evidence':f'{c:.2f}% vs prior returned period'})
    elif rev: findings.append({'topic':'Revenue','latest':rev[0]})
    else: gaps.append('revenue trend')
    if len(profit)>=2:
        c=pct_change(parse_num(profit[0].get('value')),parse_num(profit[1].get('value')))
        if c is not None: findings.append({'topic':'Net profit trend','latest':profit[0],'change_vs_previous_pct':round(c,2)})
        if c is not None and c<0: flags.append({'level':'attention','check':'net profit declined','evidence':f'{c:.2f}% vs prior returned period'})
    elif profit: findings.append({'topic':'Net profit','latest':profit[0]})
    else: gaps.append('net profit trend')
    if op and profit:
        o=parse_num(op[0].get('value')); n=parse_num(profit[0].get('value'))
        if o is not None and n is not None and o!=0: findings.append({'topic':'Profit quality context','operating_profit':o,'net_profit':n,'period':op[0].get('period')})
    if ocf:
        latest=parse_num(ocf[0].get('value'))
        findings.append({'topic':'Operating cash flow','latest':ocf[0]})
        if latest is not None and latest<0: flags.append({'level':'attention','check':'negative operating cash flow','evidence':f'{latest:g} crore'})
        if profit and latest is not None and parse_num(profit[0].get('value')) is not None:
            np=parse_num(profit[0].get('value'))
            findings.append({'topic':'Cash vs profit','operating_cash_flow':latest,'net_profit':np,'period':ocf[0].get('period'),'interpretation':'descriptive comparison only'})
    else: gaps.append('operating cash flow')
    bs=(bundle.get('balance_sheet',{}).get('data') or {}) if bundle.get('balance_sheet',{}).get('status')=='success' else {}
    bh=bs.get('history') or []
    if bh:
        a=parse_num(bh[0].get('total_asset')); l=parse_num(bh[0].get('total_liability'))
        findings.append({'topic':'Balance sheet','latest_period':bh[0].get('period'),'assets_crore':a,'liabilities_crore':l})
        if len(bh)>=2:
            lc=pct_change(l,parse_num(bh[1].get('total_liability')))
            if lc is not None: findings[-1]['liabilities_change_vs_previous_pct']=round(lc,2)
            if lc is not None and lc>20: flags.append({'level':'attention','check':'liabilities increased materially','evidence':f'{lc:.2f}% vs prior returned period'})
    else: gaps.append('balance sheet')
    sh=bundle.get('share_holdings',{}).get('data') if bundle.get('share_holdings',{}).get('status')=='success' else []
    prom=next((x for x in sh or [] if x.get('category')=='promoters'),None)
    if prom and prom.get('history'):
        h=prom['history']; latest=parse_num(h[0].get('value')); findings.append({'topic':'Promoter holding','latest':h[0]})
        if len(h)>=2:
            delta=latest-parse_num(h[1].get('value')) if latest is not None and parse_num(h[1].get('value')) is not None else None
            findings[-1]['change_pp_vs_previous_quarter']=round(delta,2) if delta is not None else None
            if delta is not None and delta<-2: flags.append({'level':'attention','check':'promoter holding decreased','evidence':f'{delta:+.2f} percentage points'})
    else: gaps.append('promoter holding')
    ratios=bundle.get('key_ratios',{}).get('data') if bundle.get('key_ratios',{}).get('status')=='success' else []
    if ratios:
        findings.append({'topic':'Key ratios','ratios':ratios})
    else: gaps.append('key ratios')
    actions=bundle.get('corporate_actions',{}).get('data') if bundle.get('corporate_actions',{}).get('status')=='success' else []
    findings.append({'topic':'Corporate actions','count':len(actions or [])})
    if not actions: gaps.append('corporate actions')
    items=(news or {}).get('items') or []
    news_hits=[]
    keywords=['result','earnings','dividend','bonus','split','acquisition','merger','order','rating','fraud','regulatory','investigation','pledge','promoter']
    for item in items:
        title=(item.get('title') or '').lower()
        hit=[k for k in keywords if k in title]
        if hit: news_hits.append({'title':item.get('title'),'keywords':hit,'published':item.get('published'),'link':item.get('link')})
    findings.append({'topic':'News event matching','items_scanned':len(items),'event_like_items':news_hits[:10]})
    if not items: gaps.append('news feed')
    return {'findings':findings,'flags':flags,'gaps':gaps,'method':'descriptive trend/event analysis; thresholds are screening flags, not investment advice'}


def historical_candles(symbol, unit='days', interval='1', to_date=None, from_date=None):
    inst=find_nse_equity(symbol)
    if not inst: return {'status':'not_found','symbol':symbol,'provider':'Upstox'}
    key=inst['instrument_key']
    end=to_date or datetime.now().date().isoformat()
    start=from_date or (datetime.now().date()-timedelta(days=365)).isoformat()
    def f():
        url=f'{UPSTOX_BASE}/v3/historical-candle/{urlquote(key,safe="")}/{unit}/{interval}/{end}/{start}'
        data=api_json(url,headers=up_headers())
        candles=(data.get('data') or {}).get('candles') or []
        return {'status':'ok','provider':'Upstox Historical Candle V3','symbol':symbol,'instrument_key':key,'unit':unit,'interval':interval,'from_date':start,'to_date':end,'candles':normalize_candles(candles)}
    return cached(f'hist:{key}:{unit}:{interval}:{start}:{end}',f)


def historical_anomaly_bundle(symbol, news=None, unit='days', interval='1', to_date=None, from_date=None):
    h=historical_candles(symbol,unit,interval,to_date,from_date)
    if h.get('status')!='ok': return {'history':h,'anomaly':{'status':'gap','reason':h.get('reason','historical data unavailable')},'news_match':{'status':'gap'}}
    a=analyze_candles(h.get('candles') or [])
    m=match_news_to_anomalies(a.get('anomalies') or [], (news or {}).get('items') or [])
    return {'history':h,'anomaly':a,'news_match':m,'generated_at':now()}

def source_map(symbol):
    return [
      {'name':'NSE Corporate Filings','url':'https://www.nseindia.com/companies-listing/corporate-filings-application','purpose':'Announcements, actions, meetings, results, shareholding'},
      {'name':'NSE Financial Results','url':'https://www.nseindia.com/companies-listing/corporate-filings-financial-results','purpose':'Financial results / XBRL'},
      {'name':'NSE Shareholding Pattern','url':'https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern','purpose':'Promoter/public shareholding and related disclosures'},
      {'name':'NSE Corporate Actions','url':'https://www.nseindia.com/companies-listing/corporate-filings-actions','purpose':'Dividends, corporate actions and meetings'},
    ]

def verify_official_sources(symbol):
    """Check official NSE source reachability/freshness only; does not scrape or infer facts."""
    results=[]
    for src in source_map(symbol):
        t0=datetime.datetime.now(datetime.timezone.utc).timestamp()
        try:
            r=requests.get(src['url'],timeout=10,headers={'User-Agent':'PersonalStockResearch/16.0'},allow_redirects=True)
            ms=round((datetime.datetime.now(datetime.timezone.utc).timestamp()-t0)*1000)
            results.append({**src,'status':'reachable' if r.status_code<400 else 'http_error','http_status':r.status_code,'latency_ms':ms,'checked_at':now()})
        except Exception as e:
            results.append({**src,'status':'unreachable','error':str(e),'checked_at':now()})
    return {'status':'success','symbol':symbol,'checked_at':now(),'sources':results,'method':'reachability check only; source contents are not treated as verified facts unless separately retrieved and parsed'}

def intraday_live(symbol, unit='minutes', interval='5'):
    inst=find_nse_equity(symbol)
    if not inst: return {'status':'gap','symbol':symbol,'reason':'instrument not found'}
    key=inst['instrument_key']
    url=f'{UPSTOX_BASE}/v3/historical-candle/intraday/{urlquote(str(key),safe="")}/{unit}/{interval}'
    data=api_json(url,headers=up_headers())
    candles=normalize_candles((data.get('data') or {}).get('candles') or [])
    return {'status':'success','provider':'Upstox Intraday Candle V3','symbol':symbol,'instrument_key':key,'unit':unit,'interval':interval,'candles':candles,'fetched_at':now()}

def event_similarity(symbol, move_pct, volume_ratio, flags):
    db_init(); con=db_connect(); con.row_factory=sqlite3.Row
    rows=[dict(r) for r in con.execute("SELECT symbol,timestamp,move_pct,volume_ratio,flags,news_count FROM research_events WHERE symbol=? ORDER BY id DESC LIMIT 200",(symbol,)).fetchall()]
    con.close()
    def dist(r):
        try:
            m=float(r.get('move_pct') or 0); v=float(r.get('volume_ratio') or 0)
            target_m=float(move_pct or 0); target_v=float(volume_ratio or 0)
            dm=abs(m-target_m)/(abs(target_m)+1.0); dv=abs(v-target_v)/(abs(target_v)+1.0)
            old=set(json.loads(r.get('flags') or '[]')); new=set(flags or [])
            overlap=len(old & new)/max(1,len(old|new))
            return dm+dv+(1-overlap)
        except Exception:return 999
    out=[]
    for r in rows:
        r['similarity_distance']=round(dist(r),4); out.append(r)
    out.sort(key=lambda x:x['similarity_distance'])
    return {'status':'success','symbol':symbol,'matches':out[:10],'method':'descriptive feature-distance using stored move, volume-ratio and flag overlap; not predictive'}


DEFAULT_WATCHLIST=['RELIANCE','TCS','INFY','HDFCBANK','ICICIBANK','SBIN','ITC','BHARTIARTL','TATAMOTORS','ADANIENT','LT','MARUTI','AXISBANK','KOTAKBANK','SUNPHARMA','WIPRO','HINDUNILVR','M&M','BAJFINANCE','NTPC']
VOICE_ALIASES={
 'reliance':'RELIANCE','रिलायंस':'RELIANCE','reliance industries':'RELIANCE','tcs':'TCS','टीसीएस':'TCS','tata consultancy':'TCS',
 'infosys':'INFY','इन्फोसिस':'INFY','infy':'INFY','itc':'ITC','आईटीसी':'ITC','sbi':'SBIN','एसबीआई':'SBIN','state bank':'SBIN',
 'hdfc bank':'HDFCBANK','एचडीएफसी बैंक':'HDFCBANK','icici bank':'ICICIBANK','आईसीआईसीआई बैंक':'ICICIBANK',
 'airtel':'BHARTIARTL','एयरटेल':'BHARTIARTL','tata motors':'TATAMOTORS','adani enterprises':'ADANIENT','adani':'ADANIENT',
 'larsen':'LT','maruti':'MARUTI','axis bank':'AXISBANK','kotak bank':'KOTAKBANK','sun pharma':'SUNPHARMA','wipro':'WIPRO','hindustan unilever':'HINDUNILVR','hdfc':'HDFCBANK','bajaj finance':'BAJFINANCE','ntpc':'NTPC'
}

def voice_symbol(text, fallback=None):
    t=(text or '').lower()
    for alias,sym in sorted(VOICE_ALIASES.items(), key=lambda x:-len(x[0])):
        if alias in t: return sym
    import re
    tokens=re.findall(r'\b[A-Z][A-Z0-9&.-]{1,14}\b', text or '')
    stop={'BACKTEST','HISTORICAL','RESEARCH','ANALYSIS','SCAN','STOCK','STOCKS','INTRADAY','VERIFY','OFFICIAL','SOURCE','SOURCES','EVENTS','EVENT','HISTORY','SHOW','RUN','DO','THE','AND','FOR','OF','COMPARE','NEWS','OLD','PAST','COMPLETE','FULL','TODAY','REPORT','MONITOR'}
    for x in tokens:
        if x not in stop: return x
    return fallback

def plan_voice_command(text, context=None):
    t=(text or '').strip(); lo=t.lower(); context=context or {}; last=context.get('symbol')
    sym=voice_symbol(t,last)
    actions=[]
    def add(a):
        if a not in actions: actions.append(a)
    if any(x in lo for x in ['20 stock','20-stock','बीस stock','market scan','stock scan','scan करो','scan कर','स्कैन']): add('scan')
    if any(x in lo for x in ['पूरी research','full research','complete research','deep research','पूरी जांच','पूरा research','सब कुछ research','all research']): add('full_research')
    if any(x in lo for x in ['research','रिसर्च','analy','जांच','जाँच']): add('research')
    if any(x in lo for x in ['backtest','बैकटेस्ट','historical validation','इतिहास में test']): add('backtest')
    if any(x in lo for x in ['intraday','इंट्राडे','intra day','5 minute','5-min']): add('intraday')
    if any(x in lo for x in ['official source','official sources','verify','verify करो','आधिकारिक source','ऑफिशियल source']): add('verify')
    if any(x in lo for x in ['old event','old events','पुराने event','पुराने events','past event','पिछले event','events देख','पुराना इतिहास']): add('events')
    if any(x in lo for x in ['compare','तुलना','similar','मिलान','पिछले जैसे','पुराने जैसे']): add('compare')
    if any(x in lo for x in ['evidence card','सबूत','evidence']): add('card')
    if any(x in lo for x in ['history','memory','रिसर्च history','रिसर्च का इतिहास','पुरानी research']): add('history')
    if any(x in lo for x in ['monitor','निगरानी','देखते रहो','लगातार देख','alert','अलर्ट']): add('monitor')
    if any(x in lo for x in ['news','न्यूज','खबर']): add('news')
    if not actions: add('help')
    if 'full_research' in actions:
        actions=[a for a in actions if a not in ('research','news','intraday','verify','events','compare','card','backtest')]
    return {'symbol':sym,'actions':actions,'text':t,'context_used':bool(last and sym==last),'allowed_actions':['research','news','intraday','verify','events','compare','card','backtest','scan','full_research','history','monitor'],'generated_at':now()}

def build_full_research(symbol, include_backtest=True):
    s=symbol.strip().upper(); started=now(); out={'status':'success','symbol':s,'started_at':started,'generated_at':now(),'stages':{},'gaps':[],'method':'evidence-first descriptive research; no ranking, score, buy/sell recommendation, or causal inference'}
    try:
        n=google_news(s); out['stages']['news']=n
        q=upstox_quote(s); out['stages']['quote']=q
        if q.get('status')!='ok': out['gaps'].append('live quote')
        bundle=fundamentals_bundle(q.get('isin')); out['stages']['fundamentals']=bundle
        out['stages']['analysis']=deep_analysis(s,bundle,n)
        out['stages']['risk']=risk_engine(q,n)
        hist=historical_anomaly_bundle(s,n); out['stages']['historical_anomalies']=hist
        try: out['stages']['official_sources']=verify_official_sources(s)
        except Exception as e: out['stages']['official_sources']={'status':'error','error':str(e)}; out['gaps'].append('official source verification')
        try:
            live=intraday_live(s,'minutes','5'); out['stages']['intraday']={'history':live,'timeline':build_intraday_timeline(live.get('candles') or [],n.get('items') or []) if live.get('status')=='success' else {'status':'gap'}}
        except Exception as e: out['stages']['intraday']={'status':'error','error':str(e)}; out['gaps'].append('intraday')
        ev=event_similarity(s,0,0,[]); out['stages']['similar_events']=ev
        if include_backtest:
            try:
                bh=historical_candles(s,'days','1')
                out['stages']['backtest']=run_backtest(bh.get('candles') or [],{'horizons':[1,3,5,10],'volume_window':20,'return_window':20,'volume_threshold':2,'move_threshold':3,'z_threshold':2,'gap_threshold':2}) if bh.get('status')=='ok' else {'status':'data-gap','history':bh}
            except Exception as e: out['stages']['backtest']={'status':'error','error':str(e)}
        out['gaps'] += out['stages']['analysis'].get('gaps',[]) if isinstance(out['stages'].get('analysis'),dict) else []
        out['gaps']=list(dict.fromkeys(out['gaps']))
        out['quality']='complete' if not out['gaps'] else 'partial'
        payload={'quote':q,'news':n,'fundamentals':bundle,'deep_analysis':out['stages']['analysis'],'risk_engine':out['stages']['risk']}
        try: build_v17_evidence(s,payload); out['evidence_recorded']=True
        except Exception as e: out['evidence_recorded']=False; out['storage_warning']=str(e)
        out['generated_at']=now(); return out
    except Exception as e:
        return {'status':'error','symbol':s,'error':str(e),'generated_at':now(),'stages':out.get('stages',{})}

def monitor_symbols(symbols):
    db_init(); rows=[]; alerts=[]; con=db_connect(); con.row_factory=sqlite3.Row
    threshold=float(os.getenv('MOVEMENT_ALERT_PCT','1.0'))
    for s in symbols[:20]:
        try:
            q=upstox_quote(s); n=google_news(s); latest=(n.get('items') or [])[:5]
            prev=con.execute("SELECT payload FROM monitoring_alerts WHERE symbol=? ORDER BY id DESC LIMIT 1",(s,)).fetchone()
            prev_price=None
            if prev:
                try: prev_price=json.loads(prev['payload']).get('quote',{}).get('last_price')
                except Exception: pass
            lp=q.get('last_price'); move_from_prev=None
            if isinstance(lp,(int,float)) and isinstance(prev_price,(int,float)) and prev_price:
                move_from_prev=round((lp/prev_price-1)*100,3)
            fp=evidence_fingerprint(json.dumps({'price':lp,'news':[(x.get('title'),x.get('published')) for x in latest]},sort_keys=True,default=str),s)
            exists=con.execute('SELECT 1 FROM monitoring_alerts WHERE symbol=? AND fingerprint=? LIMIT 1',(s,fp)).fetchone()
            movement=move_from_prev is not None and abs(move_from_prev)>=threshold
            news_change=bool(latest) and (not prev or json.dumps(latest,sort_keys=True,default=str) not in (prev['payload'] or ''))
            new_alert=(not exists) and (movement or news_change or not prev)
            alert_type='movement' if movement else ('news_change' if news_change else 'snapshot_change')
            row={'symbol':s,'status':q.get('status'),'last_price':lp,'previous_close':q.get('previous_close'),'news_count':len(n.get('items') or []),'fingerprint':fp,'new_alert':new_alert,'movement_from_previous_pct':move_from_prev,'alert_type':alert_type}
            if new_alert:
                summary=(f"{s}: observed movement {move_from_prev:+.2f}% since previous monitor snapshot" if movement else f"{s}: new observed market/news snapshot")
                payload={'quote':q,'news':latest,'movement_from_previous_pct':move_from_prev,'trigger':alert_type}
                con.execute('INSERT INTO monitoring_alerts(symbol,fingerprint,alert_type,summary,created_at,payload) VALUES(?,?,?,?,?,?)',(s,fp,alert_type,summary,now(),json.dumps(payload,default=str)))
                alerts.append({'symbol':s,'summary':summary,'created_at':now(),'alert_type':alert_type,'movement_from_previous_pct':move_from_prev,'payload':payload})
            rows.append(row)
        except Exception as e: rows.append({'symbol':s,'status':'error','error':str(e),'new_alert':False})
    con.commit(); con.close(); return {'status':'success','generated_at':now(),'rows':rows,'new_alerts':alerts,'movement_threshold_pct':threshold,'note':'Monitoring detects observed changes; it does not infer causation or produce buy/sell signals.'}

@app.after_request
def response_headers(resp):
    if request.path.startswith('/api/') or request.path in ('/','/sw.js','/manifest.json'):
        resp.headers['Cache-Control']='no-store, no-cache, must-revalidate, max-age=0'
        resp.headers['Pragma']='no-cache'
        resp.headers['Expires']='0'
    return resp

@app.errorhandler(Exception)
def handle_unexpected_error(exc):
    if request.path.startswith('/api/'):
        return jsonify({'status':'error','error':'Internal server error','detail':str(exc)}),500
    return 'Internal server error', 500

@app.get('/')
def index(): return app.send_static_file('index.html')
@app.get('/api/health')
def health():
    db_status='ok'
    db_error=None
    try:
        db_init(); con=db_connect(); con.execute('SELECT 1').fetchone(); con.close()
    except Exception as e:
        db_status='error'; db_error=str(e)
    status='ok' if db_status=='ok' else 'degraded'
    payload={'status':status,'time':now(),'upstox_configured':bool(os.getenv('UPSTOX_ACCESS_TOKEN','').strip()),'database':db_status,'news_discovery':'google_news_rss','nse_sources':'official_links','fundamentals':'upstox_company_fundamentals','filings':'official_source_links; authorized API required for automated ingestion'}
    if db_error: payload['database_error']=db_error
    return jsonify(payload), (200 if status=='ok' else 503)
@app.get('/api/instrument')
def instrument():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    try:return jsonify({'fetched_at':now(),'data':find_nse_equity(s)})
    except Exception as e:return jsonify({'status':'error','error':str(e)}),502
@app.get('/api/live-tick')
def live_tick():
    """Fast market snapshot for the Live Research screen. Uses the provider's current quote/depth snapshot; it does not fabricate data."""
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'status':'error','error':'symbol required'}),400
    try:
        q=upstox_quote(s)
        d=upstox_intraday_depth(s)
        lp,pc=q.get('last_price'),q.get('previous_close')
        change=((lp/pc)-1)*100 if isinstance(lp,(int,float)) and isinstance(pc,(int,float)) and pc else None
        return jsonify({'status':'success','symbol':s,'quote':q,'depth':d,'change_pct':change,'fetched_at':now(),'provider':'Upstox'})
    except Exception as e:
        return jsonify({'status':'error','symbol':s,'error':str(e),'fetched_at':now()}),502

@app.get('/api/quote')
def quote():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    try:q=upstox_quote(s);q['fetched_at']=now();return jsonify(q)
    except Exception as e:return jsonify({'status':'error','symbol':s,'error':str(e)}),502
@app.get('/api/fundamentals')
def fundamentals():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    try:
        inst=find_nse_equity(s); isin=(inst or {}).get('isin')
        return jsonify({'symbol':s,'isin':isin,'data':fundamentals_bundle(isin)})
    except Exception as e:return jsonify({'status':'error','symbol':s,'error':str(e)}),502
@app.get('/api/news')
def news():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    try:n=google_news(s);n.update({'symbol':s,'fetched_at':now()});return jsonify(n)
    except Exception as e:return jsonify({'status':'error','symbol':s,'error':str(e)}),502
def research_payload(s):
    """Build the core research dossier as a plain dict for routes and scanners."""
    s=(s or '').strip().upper()
    if not s:
        return {'status':'error','error':'symbol required'}
    out={'status':'success','symbol':s,'generated_at':now(),'pipeline':{}}
    try:out['quote']=upstox_quote(s);out['pipeline']['quote']='ok' if out['quote'].get('status')=='ok' else 'gap'
    except Exception as e:out['quote']={'status':'error','symbol':s,'error':str(e)};out['pipeline']['quote']='error'
    try:out['news']=google_news(s);out['pipeline']['news']='ok' if out['news'].get('items') else 'gap'
    except Exception as e:out['news']={'status':'error','symbol':s,'error':str(e)};out['pipeline']['news']='error'
    isin=(out.get('quote') or {}).get('isin')
    try:
        bundle=fundamentals_bundle(isin)
        out['fundamentals']=bundle
        out['ownership']=bundle.get('share_holdings',{'status':'gap'})
        out['results']=bundle.get('income_statement',{'status':'gap'})
        out['cash_flow']=bundle.get('cash_flow',{'status':'gap'})
        out['balance_sheet']=bundle.get('balance_sheet',{'status':'gap'})
        out['ratios']=bundle.get('key_ratios',{'status':'gap'})
        out['corporate_actions']=bundle.get('corporate_actions',{'status':'gap'})
        out['pipeline']['fundamentals']='ok' if bundle.get('status')=='ok' else 'gap'
        out['pipeline']['ownership']='ok' if bundle.get('share_holdings',{}).get('status')=='success' else 'gap'
        out['pipeline']['results']='ok' if bundle.get('income_statement',{}).get('status')=='success' else 'gap'
        out['pipeline']['cash_flow']='ok' if bundle.get('cash_flow',{}).get('status')=='success' else 'gap'
        out['fundamental_risk']=fundamental_risk(bundle)
    except Exception as e:
        out['fundamentals']={'status':'error','error':str(e)};out['pipeline']['fundamentals']='error'
        out['ownership']={'status':'gap'};out['results']={'status':'gap'};out['cash_flow']={'status':'gap'};out['balance_sheet']={'status':'gap'};out['ratios']={'status':'gap'};out['corporate_actions']={'status':'gap'}
        out['fundamental_risk']={'flags':[],'facts':[],'method':'unavailable'}
    out['filings']={'status':'source_links_only','sources':source_map(s)};out['pipeline']['filings']='source_links'
    out['analysis_report']=analyst_report(s,out.get('quote',{}),out.get('news',{}),out.get('fundamentals',{}))
    try:
        out['historical_anomalies']=historical_anomaly_bundle(s,out.get('news',{}))
        out['pipeline']['historical_price_volume']='ok' if out['historical_anomalies'].get('anomaly',{}).get('status')=='success' else 'gap'
        out['pipeline']['news_price_matching']='ok' if out['historical_anomalies'].get('news_match',{}).get('status')=='success' else 'gap'
    except Exception as e:
        out['historical_anomalies']={'status':'error','error':str(e)}
        out['pipeline']['historical_price_volume']='error'; out['pipeline']['news_price_matching']='error'
    out['deep_analysis']=deep_analysis(s,out.get('fundamentals',{}),out.get('news',{})); out['risk_engine']=risk_engine(out.get('quote',{}),out.get('news',{}));out['risk_engine']['flags'] += out.get('fundamental_risk',{}).get('flags',[]); out['risk_engine']['flags'] += out['deep_analysis'].get('flags',[]); out['risk_engine']['facts'] += out.get('fundamental_risk',{}).get('facts',[]); out['risk_engine']['not_checked']=[x for x in out['risk_engine'].get('not_checked',[]) if x not in ['debt','promoter pledge','results trend','cash flow']]; out['pipeline']['risk']='ok'
    return out

@app.get('/api/research')
def research():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    try:
        return jsonify(research_payload(s))
    except Exception as e:
        return jsonify({'status':'error','symbol':s,'error':str(e),'generated_at':now()}),502

@app.get('/api/live-research')
def live_research_api():
    """Main unified live-research endpoint: quote + depth + 5m intraday + news + fundamentals + historical evidence."""
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'status':'error','error':'symbol required'}),400
    started=now()
    out=research_payload(s)
    try:
        out['market_depth']=upstox_intraday_depth(s)
    except Exception as e:
        out['market_depth']={'status':'error','symbol':s,'provider':'Upstox','error':str(e)}
    try:
        live=intraday_live(s,'minutes','5')
        out['intraday_5m']=live
        out['intraday_timeline']=build_intraday_timeline(live.get('candles') or [], (out.get('news') or {}).get('items') or []) if live.get('status')=='success' else {'status':'gap','reason':live.get('reason') or live.get('error')}
    except Exception as e:
        out['intraday_5m']={'status':'error','symbol':s,'error':str(e)}
        out['intraday_timeline']={'status':'error','error':str(e)}
    q=out.get('quote') or {}
    out['freshness']={
        'generated_at':started,
        'quote_asof':q.get('last_trade_time') or q.get('fetched_at'),
        'depth_fetched_at':(out.get('market_depth') or {}).get('fetched_at'),
        'intraday_fetched_at':(out.get('intraday_5m') or {}).get('fetched_at'),
        'news_fetched_at':(out.get('news') or {}).get('fetched_at'),
        'label':'live where provider supplies live data; delayed/gap components are explicitly labelled'
    }
    out['live_research_sections']=['quote','day_change','volume','market_depth','intraday_5m','news','official_sources','historical_anomalies','fundamentals','deep_analysis','risk_engine','freshness']
    out['note']='Unified evidence-first live research. No buy/sell recommendation or ranking.'
    return jsonify(out)

@app.get('/api/research-batch')
def batch():
    raw=request.args.get('symbols',''); syms=[x.strip().upper() for x in raw.split(',') if x.strip()][:20]
    if not syms:return jsonify({'error':'symbols required'}),400
    rows=[]
    for s in syms:
        row={'symbol':s,'status':'error'}
        try:
            q=upstox_quote(s); lp,pc=q.get('last_price'),q.get('previous_close'); ch=round((lp/pc-1)*100,2) if isinstance(lp,(int,float)) and isinstance(pc,(int,float)) and pc else None
            row.update({'status':q.get('status'),'last_price':lp,'change_pct':ch,'volume':q.get('volume'),'year_high':q.get('year_high'),'year_low':q.get('year_low'),'isin':q.get('isin')})
            if q.get('isin'):
                b=fundamentals_bundle(q.get('isin'))
                a=deep_analysis(s,b,{'items':[]})
                row['fundamental_flags']=[f['check'] for f in a.get('flags',[])][:6]
                row['research_gaps']=a.get('gaps',[])[:6]
                row['latest_revenue']=next((f['latest'].get('value') for f in a['findings'] if f.get('topic')=='Revenue trend' and f.get('latest')),None)
                row['latest_net_profit']=next((f['latest'].get('value') for f in a['findings'] if f.get('topic')=='Net profit trend' and f.get('latest')),None)
                row['promoter_holding']=next((f['latest'].get('value') for f in a['findings'] if f.get('topic')=='Promoter holding' and f.get('latest')),None)
        except Exception as e: row['error']=str(e)
        row['fetched_at']=now(); rows.append(row)
    return jsonify({'generated_at':now(),'count':len(rows),'results':rows})


@app.get('/api/history')
def history_api():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    unit=request.args.get('unit','days'); interval=request.args.get('interval','1')
    to_date=request.args.get('to_date') or datetime.now().date().isoformat()
    from_date=request.args.get('from_date') or (datetime.now().date()-timedelta(days=365)).isoformat()
    try:return jsonify(historical_candles(s,unit,interval,to_date,from_date))
    except Exception as e:return jsonify({'status':'error','symbol':s,'error':str(e)}),502

@app.get('/api/intraday-timeline')
def intraday_timeline_api():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    unit=request.args.get('unit','minutes'); interval=request.args.get('interval','5')
    to_date=request.args.get('to_date') or datetime.now().date().isoformat()
    from_date=request.args.get('from_date') or to_date
    try:
        h=historical_candles(s,unit,interval,to_date,from_date)
        if h.get('status')!='ok': return jsonify({'history':h,'timeline':{'status':'gap'}})
        n=google_news(s)
        t=build_intraday_timeline(h.get('candles') or [], n.get('items') or [])
        return jsonify({'history':h,'timeline':t,'generated_at':now()})
    except Exception as e:return jsonify({'status':'error','symbol':s,'error':str(e)}),502


@app.get('/api/intraday-scan')
def intraday_scan_api():
    raw=request.args.get('symbols','')
    syms=[x.strip().upper() for x in raw.split(',') if x.strip()][:20]
    if not syms:return jsonify({'status':'error','error':'symbols required'}),400
    unit=request.args.get('unit','minutes'); interval=request.args.get('interval','5')
    rows=[]
    for s in syms:
        row={'symbol':s,'status':'gap','candles_analyzed':0,'anomaly_count':0,'events_with_news':0,
             'latest_anomaly':None,'latest_move_pct':None,'latest_volume_ratio':None,'flags':[],
             'news_count':0,'news':[],'error':None,'fetched_at':now()}
        try:
            ins=find_nse_equity(s)
            if not ins:
                row['status']='error'; row['error']='NSE instrument not found'; rows.append(row); continue
            key=ins.get('instrument_key') or ins.get('instrument_token')
            url=f'{UPSTOX_BASE}/v3/historical-candle/intraday/{urlquote(str(key),safe="")}/{unit}/{interval}'
            h=api_json(url,headers=up_headers())
            candles=normalize_candles((h.get('data') or {}).get('candles') or [])
            n=google_news(s)
            t=build_intraday_timeline(candles,n.get('items') or [])
            events=t.get('events') or []
            anomalies=[e for e in events if e.get('type')=='price_volume_anomaly']
            latest=anomalies[-1] if anomalies else None
            row.update({'status':'success','candles_analyzed':t.get('candles_analyzed',len(candles)),
                        'anomaly_count':t.get('anomaly_count',len(anomalies)),
                        'events_with_news':t.get('events_with_news',0),
                        'latest_anomaly':latest.get('timestamp') if latest else None,
                        'latest_move_pct':latest.get('return_pct') if latest else None,
                        'latest_volume_ratio':latest.get('volume_ratio') if latest else None,
                        'flags':latest.get('flags',[]) if latest else [],
                        'news_count':sum(len(e.get('news') or []) for e in anomalies),
                        'news':[(e.get('news') or []) for e in anomalies[-5:]][::-1],
                        'fetched_at':now()})
        except Exception as e:
            row['status']='error'; row['error']=str(e); row['fetched_at']=now()
        rows.append(row)
    return jsonify({'status':'success','count':len(rows),'unit':unit,'interval':interval,
                    'generated_at':now(),
                    'rows':rows,
                    'note':'Descriptive intraday anomaly scan. Temporal news association only; no causation, ranking, or buy/sell recommendation.'})

@app.get('/api/auto-research')
def auto_research_api():
    """Automatic evidence-first workflow: intraday scan first, then deeper research on symbols with anomalies/news-linked events."""
    raw=request.args.get('symbols','')
    syms=[x.strip().upper() for x in raw.split(',') if x.strip()][:20]
    if not syms:return jsonify({'status':'error','error':'symbols required'}),400
    deep_all=request.args.get('deep_all','0')=='1'
    started=now(); rows=[]
    # Stage 1: fast intraday scan.
    for s in syms:
        row={'symbol':s,'intraday':None,'research':None,'decision':'not_selected','status':'pending'}
        try:
            ins=find_nse_equity(s)
            if not ins:
                row.update({'status':'error','error':'NSE instrument not found'}); rows.append(row); continue
            key=ins.get('instrument_key') or ins.get('instrument_token')
            url=f'{UPSTOX_BASE}/v3/historical-candle/intraday/{urlquote(str(key),safe="")}/minutes/5'
            h=api_json(url,headers=up_headers())
            candles=normalize_candles((h.get('data') or {}).get('candles') or [])
            n=google_news(s)
            t=build_intraday_timeline(candles,n.get('items') or [])
            anomalies=[e for e in (t.get('events') or []) if e.get('type')=='price_volume_anomaly']
            latest=anomalies[-1] if anomalies else None
            row['intraday']={'candles_analyzed':t.get('candles_analyzed',len(candles)),'anomaly_count':t.get('anomaly_count',len(anomalies)),'events_with_news':t.get('events_with_news',0),'latest':latest,'news_count':len(n.get('items') or [])}
            selected=deep_all or bool(anomalies) or bool(t.get('events_with_news',0)) or bool(n.get('items'))
            if selected:
                row['decision']='deep_research'; # Call the underlying research logic without an HTTP round-trip.
                q=upstox_quote(s); bundle=fundamentals_bundle((q or {}).get('isin'))
                hist=historical_anomaly_bundle(s,n)
                rep=analyst_report(s,q,n,bundle)
                deep=deep_analysis(s,bundle,n); risk=risk_engine(q,n)
                risk['flags'] += bundle.get('fundamental_risk',{}).get('flags',[]) if isinstance(bundle,dict) else []
                risk['flags'] += deep.get('flags',[])
                risk['facts'] += bundle.get('fundamental_risk',{}).get('facts',[]) if isinstance(bundle,dict) else []
                row['research']={'quote':q,'news':n,'fundamentals':bundle,'historical_anomalies':hist,'analysis_report':rep,'deep_analysis':deep,'risk_engine':risk,'sources':source_map(s)}
                row['status']='success'
            else:
                row['status']='scanned_no_deep_research'
        except Exception as e:
            row['status']='error'; row['error']=str(e)
        rows.append(row)
    payload={'status':'success','generated_at':now(),'started_at':started,'count':len(rows),'deep_researched':sum(1 for r in rows if r.get('decision')=='deep_research'),'rows':rows,
                    'method':'automatic staged research: intraday/news screening followed by deeper evidence collection; temporal associations are not causal and no buy/sell/ranking output is produced.'}
    try: payload['run_id']=db_save_run(payload)
    except Exception as e: payload['storage_warning']=str(e)
    return jsonify(payload)

@app.route('/api/voice-plan', methods=['GET','POST'])
def voice_plan_api():
    try:
        body=request.get_json(silent=True) or {}
        return jsonify(plan_voice_command(body.get('text',''),body.get('context') or {}))
    except Exception as e: return jsonify({'status':'error','error':str(e)}),400

@app.get('/api/full-research')
def full_research_api():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    return jsonify(build_full_research(s, request.args.get('backtest','1')!='0'))

@app.get('/api/monitor')
def monitor_api():
    raw=request.args.get('symbols','') or ','.join(DEFAULT_WATCHLIST)
    syms=[x.strip().upper() for x in raw.split(',') if x.strip()][:20]
    return jsonify(monitor_symbols(syms))

@app.get('/api/monitor-alerts')
def monitor_alerts_api():
    db_init(); limit=min(max(int(request.args.get('limit','50')),1),200); con=db_connect(); con.row_factory=sqlite3.Row
    rows=[dict(r) for r in con.execute('SELECT * FROM monitoring_alerts ORDER BY id DESC LIMIT ?',(limit,)).fetchall()]; con.close(); return jsonify({'status':'success','alerts':rows})

@app.get('/api/research-dossier')
def research_dossier_api():
    s=(request.args.get('symbol') or '').strip().upper()
    if not s: return jsonify({'status':'error','error':'symbol required'}),400
    d=build_full_research(s, request.args.get('backtest','1')!='0')
    d['dossier_version']='V22'
    d['data_checked_at']=now()
    return jsonify(d)

@app.get('/api/monitor-alerts-summary')
def monitor_alerts_summary_api():
    db_init(); con=db_connect(); con.row_factory=sqlite3.Row
    total=con.execute('SELECT COUNT(*) c FROM monitoring_alerts').fetchone()['c']
    recent=[dict(r) for r in con.execute('SELECT * FROM monitoring_alerts ORDER BY id DESC LIMIT 20').fetchall()]
    con.close(); return jsonify({'status':'success','total':total,'recent':recent,'generated_at':now()})

@app.get('/api/export-research')
def export_research_api():
    s=(request.args.get('symbol') or '').strip().upper()
    if not s: return jsonify({'status':'error','error':'symbol required'}),400
    d=build_full_research(s, request.args.get('backtest','1')!='0')
    d['exported_at']=now(); d['export_version']='V22'
    return app.response_class(json.dumps(d,ensure_ascii=False,indent=2,default=str),mimetype='application/json',headers={'Content-Disposition':f'attachment; filename="{s}_research_dossier.json"'})

@app.post('/api/save-moment')
def save_moment_api():
    body=request.get_json(silent=True) or {}
    symbol=str(body.get('symbol') or '').strip().upper()
    if not symbol:return jsonify({'status':'error','error':'symbol required'}),400
    payload=body.get('payload') or body
    title=str(body.get('title') or f'{symbol} market moment')[:200]
    trigger=str(body.get('trigger') or 'manual')[:100]
    summary=str(body.get('summary') or '')[:1000]
    db_init(); con=sqlite3.connect(DB_PATH)
    cur=con.execute('INSERT INTO saved_moments(symbol,saved_at,title,trigger,summary,payload) VALUES(?,?,?,?,?,?)',(symbol,now(),title,trigger,summary,json.dumps(payload,ensure_ascii=False,default=str)))
    con.commit(); mid=cur.lastrowid; con.close()
    return jsonify({'status':'success','id':mid,'symbol':symbol,'saved_at':now()})

@app.get('/api/saved-moments')
def saved_moments_api():
    db_init(); limit=min(max(int(request.args.get('limit','50')),1),200); symbol=request.args.get('symbol','').strip().upper()
    con=db_connect(); con.row_factory=sqlite3.Row
    if symbol: rows=[dict(r) for r in con.execute('SELECT * FROM saved_moments WHERE symbol=? ORDER BY id DESC LIMIT ?',(symbol,limit)).fetchall()]
    else: rows=[dict(r) for r in con.execute('SELECT * FROM saved_moments ORDER BY id DESC LIMIT ?',(limit,)).fetchall()]
    con.close(); return jsonify({'status':'success','moments':rows})

@app.get('/api/research-history')
def research_history_api():
    db_init(); limit=min(max(int(request.args.get('limit','20')),1),100); con=db_connect(); con.row_factory=sqlite3.Row
    rows=[dict(r) for r in con.execute("SELECT id,generated_at,symbols,payload FROM research_runs ORDER BY id DESC LIMIT ?",(limit,)).fetchall()]; con.close()
    for r in rows:
        try: p=json.loads(r['payload']); r['count']=p.get('count'); r['deep_researched']=p.get('deep_researched')
        except: pass
    return jsonify({'status':'success','runs':rows})

@app.get('/api/similar-events')
def similar_events_api():
    symbol=request.args.get('symbol','').strip().upper(); db_init(); con=db_connect(); con.row_factory=sqlite3.Row
    if symbol:
        rows=[dict(r) for r in con.execute("SELECT symbol,event_type,timestamp,move_pct,volume_ratio,flags,news_count FROM research_events WHERE symbol=? ORDER BY id DESC LIMIT 50",(symbol,)).fetchall()]
    else:
        rows=[dict(r) for r in con.execute("SELECT symbol,event_type,timestamp,move_pct,volume_ratio,flags,news_count FROM research_events ORDER BY id DESC LIMIT 100").fetchall()]
    con.close(); return jsonify({'status':'success','symbol':symbol,'events':rows,'method':'historical similarity by stored event features; descriptive only'})

@app.get('/api/stock-card')
def stock_card_api():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    q=upstox_quote(s); n=google_news(s); bundle=fundamentals_bundle((q or {}).get('isin')); deep=deep_analysis(s,bundle,n); risk=risk_engine(q,n)
    return jsonify({'status':'success','symbol':s,'generated_at':now(),'quote':q,'news':n,'fundamentals':bundle,'deep_analysis':deep,'risk_engine':risk,'sources':source_map(s),'note':'Evidence card only; no ranking or buy/sell recommendation.'})

@app.get('/api/verify-sources')
def verify_sources_api():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    try:return jsonify(verify_official_sources(s))
    except Exception as e:return jsonify({'status':'error','symbol':s,'error':str(e)}),502

@app.get('/api/intraday-depth')
def intraday_depth_api():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'status':'error','error':'symbol required'}),400
    return jsonify(upstox_intraday_depth(s))

@app.get('/api/intraday-live')
def intraday_live_api():
    s=request.args.get('symbol','').strip().upper(); unit=request.args.get('unit','minutes'); interval=request.args.get('interval','5')
    if not s:return jsonify({'error':'symbol required'}),400
    try:
        x=intraday_live(s,unit,interval); t=build_intraday_timeline(x.get('candles') or [], google_news(s).get('items') or []) if x.get('status')=='success' else {'status':'gap'}
        return jsonify({'history':x,'timeline':t,'generated_at':now()})
    except Exception as e:return jsonify({'status':'error','symbol':s,'error':str(e)}),502

@app.get('/api/event-similarity')
def event_similarity_api():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    try:return jsonify(event_similarity(s,float(request.args.get('move_pct','0')),float(request.args.get('volume_ratio','0')),request.args.get('flags','').split('|') if request.args.get('flags') else []))
    except Exception as e:return jsonify({'status':'error','symbol':s,'error':str(e)}),400

@app.get('/api/v17-evidence')
def v17_evidence_api():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    limit=min(max(int(request.args.get('limit','50')),1),200); db_init(); con=db_connect(); con.row_factory=sqlite3.Row
    rows=[dict(r) for r in con.execute("SELECT * FROM evidence_ledger WHERE symbol=? ORDER BY id DESC LIMIT ?",(s,limit)).fetchall()]; con.close()
    return jsonify({'status':'success','symbol':s,'evidence':rows,'count':len(rows)})

@app.get('/api/v17-dossier')
def v17_dossier_api():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    q=upstox_quote(s); n=google_news(s); bundle=fundamentals_bundle((q or {}).get('isin')); deep=deep_analysis(s,bundle,n); risk=risk_engine(q,n)
    payload={'status':'success','symbol':s,'generated_at':now(),'quote':q,'news':n,'fundamentals':bundle,'deep_analysis':deep,'risk_engine':risk}
    build_v17_evidence(s,payload); diff=snapshot_diff(s,payload)
    db_init(); con=db_connect(); con.row_factory=sqlite3.Row
    ev=[dict(r) for r in con.execute("SELECT * FROM evidence_ledger WHERE symbol=? ORDER BY id DESC LIMIT 100",(s,)).fetchall()]; con.close()
    gaps=deep.get('gaps') or []; quality='complete' if ev and not gaps else ('partial' if ev else 'data-gap')
    return jsonify({'status':'success','symbol':s,'quality':quality,'change':diff,'evidence':ev,'research':payload,'generated_at':now()})

@app.get('/api/v17-change')
def v17_change_api():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    db_init(); con=db_connect(); con.row_factory=sqlite3.Row; rows=[dict(r) for r in con.execute("SELECT id,generated_at,summary FROM research_snapshots WHERE symbol=? ORDER BY id DESC LIMIT 10",(s,)).fetchall()]; con.close()
    return jsonify({'status':'success','symbol':s,'snapshots':rows})

@app.get('/api/v17-replay')
def v17_replay_api():
    s=request.args.get('symbol','').strip().upper(); event_id=request.args.get('event_id')
    if not s:return jsonify({'error':'symbol required'}),400
    db_init(); con=db_connect(); con.row_factory=sqlite3.Row; row=con.execute("SELECT * FROM research_events WHERE symbol=? AND (? IS NULL OR id=?) ORDER BY id DESC LIMIT 1",(s,event_id,event_id)).fetchone(); con.close()
    if not row:return jsonify({'status':'gap','error':'stored event not found'})
    x=dict(row); news=google_news(s); hist=historical_anomaly_bundle(s,news); return jsonify({'status':'success','symbol':s,'event':x,'replay':hist,'news':news,'method':'descriptive event replay; no causation inference'})

@app.get('/api/anomalies')
def anomalies_api():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    try:
        news=google_news(s)
        return jsonify(historical_anomaly_bundle(s,news,request.args.get('unit','days'),request.args.get('interval','1'),request.args.get('to_date'),request.args.get('from_date')))
    except Exception as e:return jsonify({'status':'error','symbol':s,'error':str(e)}),502




@app.get('/api/backtest')
def backtest_api():
    s=request.args.get('symbol','').strip().upper()
    if not s:return jsonify({'error':'symbol required'}),400
    try:
        unit=request.args.get('unit','days'); interval=request.args.get('interval','1')
        h=request.args.get('horizons','1,3,5,10')
        horizons=[int(x) for x in h.split(',') if x.strip().isdigit()][:8]
        cfg={'horizons':horizons or [1,3,5,10],'volume_window':int(request.args.get('volume_window','20')),'return_window':int(request.args.get('return_window','20')),'volume_threshold':float(request.args.get('volume_threshold','2')),'move_threshold':float(request.args.get('move_threshold','3')),'z_threshold':float(request.args.get('z_threshold','2')),'gap_threshold':float(request.args.get('gap_threshold','2'))}
        hist=historical_candles(s,unit,interval,request.args.get('to_date'),request.args.get('from_date'))
        if hist.get('status')!='ok': return jsonify({'status':'data-gap','symbol':s,'history':hist})
        result=run_backtest(hist.get('candles') or [],cfg)
        result.update({'symbol':s,'generated_at':now(),'data_source':hist.get('provider'),'from_date':hist.get('from_date'),'to_date':hist.get('to_date'),'unit':unit,'interval':interval})
        return jsonify(result)
    except Exception as e:return jsonify({'status':'error','symbol':s,'error':str(e)}),502

@app.get("/api/analyze")
def api_analyze():
    symbol=request.args.get('symbol','').strip().upper()
    if not symbol:return jsonify({"status":"error","error":"symbol required"}),400
    try:
        payload=research_payload(symbol)
        return jsonify({"status":"success","symbol":symbol,"analysis":build_analysis(payload)})
    except Exception as e:
        return jsonify({"status":"error","symbol":symbol,"error":str(e)}),502


@app.get("/api/scan")
def api_scan():
    raw=request.args.get('symbols','')
    syms=[x.strip().upper() for x in raw.split(",") if x.strip()][:20]
    if not syms:
        return jsonify({"status":"error","error":"Provide comma-separated symbols, maximum 20."}),400
    try:
        return jsonify(scan_stocks(syms, research_payload))
    except Exception as e:
        return jsonify({"status":"error","error":str(e)}),502

if __name__=='__main__':
    app.run(host='0.0.0.0',port=PORT,debug=False)
