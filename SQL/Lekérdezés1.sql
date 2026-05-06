USE graham_stocks;

-- 1. Összesen hány rekord
SELECT COUNT(*) AS total_companies FROM stock_fundamentals;

-- 2. Iparág-bontás
SELECT industry, COUNT(*) AS cnt
FROM stock_fundamentals
GROUP BY industry;

-- 3. Top 10 legmagasabb Graham score - nézzük, kik a "legjobb" bankok
SELECT ticker, company_name, country, market_cap_musd,
       pe_ratio, pb_ratio, dividend_yield, roe,
       graham_number, price_to_graham, graham_score
FROM stock_fundamentals
ORDER BY graham_score DESC, market_cap_musd DESC
LIMIT 10;

-- 4. Bank-specifikus szűrő (a view-t teszteljük)
SELECT ticker, company_name, pe_ratio, pb_ratio, roe, dividend_yield, graham_score
FROM vw_banks_screen
ORDER BY graham_score DESC, pb_ratio ASC
LIMIT 15;

-- 5. ETL log - itt látszik a futás története
SELECT id, run_started, run_finished, status,
       records_inserted, records_updated, records_failed,
       duration_seconds, notes
FROM etl_log
ORDER BY id DESC;