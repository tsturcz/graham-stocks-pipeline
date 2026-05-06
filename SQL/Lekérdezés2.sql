USE graham_stocks;

-- 1. Adat-minőség: hány banknál van értelmes érték az egyes mezőkben?
SELECT
    COUNT(*)                                        AS total,
    SUM(pe_ratio IS NOT NULL)                       AS has_pe,
    SUM(pe_ratio > 0 AND pe_ratio < 15)             AS pe_under_15,
    SUM(pb_ratio IS NOT NULL)                       AS has_pb,
    SUM(pb_ratio > 0 AND pb_ratio < 1.5)            AS pb_under_1_5,
    SUM(roe IS NOT NULL)                            AS has_roe,
    SUM(roe > 8)                                    AS roe_over_8,
    SUM(dividend_yield IS NOT NULL)                 AS has_div,
    SUM(dividend_yield > 0)                         AS div_paying
FROM stock_fundamentals;

-- 2. Konkrét nagy bankok ellenőrzése (kell hogy legyenek a teszt mintában)
SELECT ticker, company_name, pe_ratio, pb_ratio, roe, dividend_yield, current_ratio, eps_ttm, book_per_share
FROM stock_fundamentals
WHERE ticker IN ('JPM', 'BAC', 'C', 'WFC', 'HSBC', 'BCS', 'UBS')
ORDER BY ticker;

-- 3. Ki kerülne a screen-be ha lazítanánk a P/B kritériumon?
SELECT ticker, company_name, pe_ratio, pb_ratio, roe, dividend_yield
FROM stock_fundamentals
WHERE pe_ratio > 0 AND pe_ratio < 15
  AND pb_ratio > 0 AND pb_ratio < 2.5      -- lazább P/B
  AND roe > 8
  AND dividend_yield > 0
ORDER BY graham_score DESC, pb_ratio ASC
LIMIT 20;