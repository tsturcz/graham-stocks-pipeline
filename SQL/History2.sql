SELECT scrape_date, COUNT(*) AS rows_for_day
FROM stock_fundamentals_history
GROUP BY scrape_date
ORDER BY scrape_date;