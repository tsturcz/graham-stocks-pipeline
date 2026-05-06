SELECT ticker, company_name, pe_ratio, pb_ratio, roe, dividend_yield
FROM stock_fundamentals
WHERE ticker IN ('JPM', 'BAC', 'C', 'WFC', 'HSBC', 'BCS', 'UBS')
ORDER BY ticker;