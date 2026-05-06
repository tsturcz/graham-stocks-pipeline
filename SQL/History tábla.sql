-- ================================================================
-- Graham Stock Fundamentals - Historical Time-Series Table
-- ----------------------------------------------------------------
-- Adds a "dual-write" pattern alongside the snapshot table:
--   * stock_fundamentals          -> current snapshot (existing)
--   * stock_fundamentals_history  -> append-only time series (new)
--
-- This enables trend analysis, backtesting, and anomaly detection
-- in addition to the "what's the best stock today?" snapshot view.
--
-- Backfills the current snapshot as the first historical entry.
--
-- Author : Tamas Sturcz
-- Created: 2026-05-04
-- ================================================================

USE graham_stocks;


-- ----------------------------------------------------------------
-- 1. History table - one row per (ticker, scrape_date)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_fundamentals_history (
    id                  BIGINT          AUTO_INCREMENT PRIMARY KEY,
    scrape_date         DATE            NOT NULL,
    ticker              VARCHAR(10)     NOT NULL,

    -- Identification (denormalized for snapshot independence) --------
    company_name        VARCHAR(255),
    sector              VARCHAR(100),
    industry            VARCHAR(150),
    country             VARCHAR(50),

    -- Pricing ---------------------------------------------------------
    price               DECIMAL(12,4),
    market_cap_musd     DECIMAL(18,2),

    -- Graham core ratios ---------------------------------------------
    pe_ratio            DECIMAL(10,2),
    pb_ratio            DECIMAL(10,2),

    -- Earnings & growth ----------------------------------------------
    eps_ttm             DECIMAL(10,4),
    eps_growth_5y       DECIMAL(10,2),

    -- Balance sheet --------------------------------------------------
    current_ratio       DECIMAL(10,2),
    lt_debt_eq          DECIMAL(10,2),
    debt_eq             DECIMAL(10,2),

    -- Profitability --------------------------------------------------
    roe                 DECIMAL(10,2),
    roa                 DECIMAL(10,2),

    -- Dividends ------------------------------------------------------
    dividend_yield      DECIMAL(10,2),
    payout_ratio        DECIMAL(10,2),

    -- Per-share & calculated -----------------------------------------
    book_per_share      DECIMAL(10,4),
    sales_musd          DECIMAL(18,2),
    graham_number       DECIMAL(12,4),
    price_to_graham     DECIMAL(10,4),
    graham_score        TINYINT,

    -- Metadata --------------------------------------------------------
    inserted_at         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

    -- Constraints -----------------------------------------------------
    UNIQUE KEY uq_ticker_date (ticker, scrape_date),
    INDEX idx_scrape_date (scrape_date),
    INDEX idx_ticker (ticker),
    INDEX idx_sector_date (sector, scrape_date),
    INDEX idx_industry_date (industry, scrape_date),
    INDEX idx_graham_score_date (graham_score, scrape_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ----------------------------------------------------------------
-- 2. Backfill: copy current snapshot as the first historical entry
--    so we don't lose today's data when starting time-series tracking.
-- ----------------------------------------------------------------
INSERT IGNORE INTO stock_fundamentals_history (
    scrape_date, ticker, company_name, sector, industry, country,
    price, market_cap_musd,
    pe_ratio, pb_ratio,
    eps_ttm, eps_growth_5y,
    current_ratio, lt_debt_eq, debt_eq,
    roe, roa,
    dividend_yield, payout_ratio,
    book_per_share, sales_musd,
    graham_number, price_to_graham, graham_score
)
SELECT
    DATE(last_updated) AS scrape_date,
    ticker, company_name, sector, industry, country,
    price, market_cap_musd,
    pe_ratio, pb_ratio,
    eps_ttm, eps_growth_5y,
    current_ratio, lt_debt_eq, debt_eq,
    roe, roa,
    dividend_yield, payout_ratio,
    book_per_share, sales_musd,
    graham_number, price_to_graham, graham_score
FROM stock_fundamentals;


-- ----------------------------------------------------------------
-- 3. Time-series analysis views (for Power BI)
-- ----------------------------------------------------------------

-- Sector trends over time (avg ratios per sector per day)
CREATE OR REPLACE VIEW vw_sector_trend AS
SELECT
    scrape_date,
    sector,
    COUNT(*)                                            AS company_count,
    AVG(pe_ratio)                                       AS avg_pe,
    AVG(pb_ratio)                                       AS avg_pb,
    AVG(roe)                                            AS avg_roe,
    AVG(dividend_yield)                                 AS avg_dividend_yield,
    AVG(graham_score)                                   AS avg_graham_score,
    SUM(CASE WHEN graham_score >= 5 THEN 1 ELSE 0 END)  AS strong_picks
FROM stock_fundamentals_history
WHERE pe_ratio IS NOT NULL
GROUP BY scrape_date, sector;


-- Industry trends (more granular than sector)
CREATE OR REPLACE VIEW vw_industry_trend AS
SELECT
    scrape_date,
    industry,
    COUNT(*)                                            AS company_count,
    AVG(pe_ratio)                                       AS avg_pe,
    AVG(pb_ratio)                                       AS avg_pb,
    AVG(roe)                                            AS avg_roe,
    AVG(dividend_yield)                                 AS avg_dividend_yield
FROM stock_fundamentals_history
WHERE pe_ratio IS NOT NULL
GROUP BY scrape_date, industry;


-- Per-ticker history (use this for individual stock evolution charts)
CREATE OR REPLACE VIEW vw_ticker_history AS
SELECT
    ticker, company_name, sector, industry,
    scrape_date,
    price, pe_ratio, pb_ratio, roe, dividend_yield,
    graham_number, price_to_graham, graham_score
FROM stock_fundamentals_history;


-- ----------------------------------------------------------------
-- 4. Verification
-- ----------------------------------------------------------------
SELECT
    COUNT(*)                       AS total_history_rows,
    COUNT(DISTINCT ticker)         AS unique_tickers,
    COUNT(DISTINCT scrape_date)    AS distinct_dates,
    MIN(scrape_date)               AS first_date,
    MAX(scrape_date)               AS latest_date
FROM stock_fundamentals_history;

SHOW TABLES;