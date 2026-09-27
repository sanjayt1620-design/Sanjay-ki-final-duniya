# Personal Stock Research App V7

V7 adds a deeper evidence-first analysis layer on top of the V6 Indian-market research app.

## Added in V7
- Quarter/year trend checks for revenue and net profit.
- Operating cash-flow vs profit context.
- Balance-sheet liability trend flags.
- Promoter holding quarter-over-quarter change flags.
- Key-ratio/company-vs-sector context from Upstox fundamentals.
- Corporate-action count in the report.
- News event keyword matching for results, dividends, bonus, split, acquisition, orders, ratings, regulatory/investigation and promoter/pledge terms.
- 20-stock scanner now fetches selected fundamental fields and rule-based flags.
- No fabricated values; missing provider data remains a gap.
- The analysis is descriptive screening, not a buy/sell recommendation.

## Run
1. `python -m venv .venv`
2. Activate it.
3. `pip install -r requirements.txt`
4. Set `UPSTOX_ACCESS_TOKEN` in `.env` or the environment.
5. `python server.py`
6. Open `http://127.0.0.1:8787`

Upstox fundamentals are accessed by ISIN and currently document company profile, balance sheet, cash flow, income statement, share holdings, key ratios, corporate actions and competitors.


## V8 additions
- Transparent comparison/anomaly analysis endpoint: `/api/analyze?symbol=RELIANCE`
- Revenue/profit trend checks
- Operating cash flow vs profit check
- Promoter holding change check
- Sector-relative ratio comparison
- Explicit data-gap reporting
- No buy/sell score or fabricated values


## V9 additions
- `/api/scan?symbols=RELIANCE,TCS,INFY` for up to 20 stocks
- Multi-stock descriptive dashboard
- Per-stock flags, data gaps, stage status and freshness
- No ranking, score, or buy/sell recommendation

## V10 — Historical Price + Volume Anomaly Engine
- Upstox Historical Candle V3 integration for daily/other supported intervals.
- Price-return, volume-ratio, return z-score, opening-gap and range checks.
- News-to-price event matching using a transparent +/- 1 trading-calendar-day date window.
- No ranking, score, buy/sell output, or causal claim.
- Missing history/news remains explicitly marked as a gap.

## V26 — Live Research
- Live Research is the primary screen and calls `/api/live-research`.
- Fast `/api/live-tick` endpoint refreshes provider quote + visible market depth without rerunning the full dossier.
- Mobile UI includes a 3-second Live Auto mode and recent live-price movement strip.
- Live values are shown only when the configured market-data provider returns them; unavailable/delayed components remain explicit gaps.
- Upstox V3 market data is the configured provider surface. Its WebSocket feed is the path for continuous streaming; the current app uses provider snapshots for reliable server-side research and fast refresh.
- Set `UPSTOX_ACCESS_TOKEN` as a deployment secret. Never commit the token to the ZIP or source.
- The app remains evidence-first and does not turn live movement into a buy/sell recommendation.
