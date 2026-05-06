USE graham_stocks;

SELECT ticker, company_name, sector, industry, country,
       last_updated, market_cap_musd, pe_ratio, dividend_yield
FROM stock_fundamentals
WHERE DATE(last_updated) = '2026-05-03';