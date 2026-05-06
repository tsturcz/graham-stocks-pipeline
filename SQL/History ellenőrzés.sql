USE graham_stocks;

-- 1. Mennyi sor van a history-ban összesen, hány különböző tickerrel
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT ticker) AS unique_tickers,
    MIN(scrape_date) AS first_date,
    MAX(scrape_date) AS latest_date
FROM stock_fundamentals_history;

-- 2. Az új ticker keresése (ami most este 19:53-19:54-kor került be)
SELECT *
FROM stock_fundamentals_history
WHERE inserted_at >= '2026-05-04 19:50:00'
ORDER BY inserted_at DESC
LIMIT 5;