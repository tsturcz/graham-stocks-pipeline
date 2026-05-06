etl_log-- ================================================================
-- Graham Stock Fundamentals - Database Schema
-- ----------------------------------------------------------------
-- Project: Automated stock fundamentals tracking from Finviz
-- Author : Tamas Sturcz
-- Created: 2026-05-03
-- Purpose: Daily snapshot of Graham-relevant fundamentals
--          for selected sectors (banks, auto, mining,
--          consumer defensive, utilities, industrials)
-- ================================================================

-- ----------------------------------------------------------------
-- 1. Database creation
-- ----------------------------------------------------------------
DROP DATABASE IF EXISTS graham_stocks;
CREATE DATABASE graham_stocks
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE graham_stocks;


-- ----------------------------------------------------------------
-- 2. Main table: latest fundamentals snapshot per ticker
-- ----------------------------------------------------------------
CREATE TABLE stock_fundamentals (
    -- Identification ------------------------------------------------
    ticker              VARCHAR(10)    NOT NULL,
    company_name        VARCHAR(255)   NOT NULL,
    sector              VARCHAR(100),
    industry            VARCHAR(150),
    country             VARCHAR(50),

    -- Pricing -------------------------------------------------------
    price               DECIMAL(12,4)  COMMENT 'Latest closing price (USD)',
    market_cap_musd     DECIMAL(18,2)  COMMENT 'Market cap in millions USD',

    -- Graham core ratios -------------------------------------------
    pe_ratio            DECIMAL(10,2)  COMMENT 'Graham: < 15',
    pb_ratio            DECIMAL(10,2)  COMMENT 'Graham: < 1.5',

    -- Earnings & growth --------------------------------------------
    eps_ttm             DECIMAL(10,4)  COMMENT 'Earnings per share (trailing 12M)',
    eps_growth_5y       DECIMAL(10,2)  COMMENT 'EPS growth past 5 years (%)',

    -- Balance sheet strength ---------------------------------------
    current_ratio       DECIMAL(10,2)  COMMENT 'Graham: > 2 (not for banks)',
    lt_debt_eq          DECIMAL(10,2)  COMMENT 'Long-term debt to equity',
    debt_eq             DECIMAL(10,2)  COMMENT 'Total debt to equity',

    -- Profitability ------------------------------------------------
    roe                 DECIMAL(10,2)  COMMENT 'Return on equity (%)',
    roa                 DECIMAL(10,2)  COMMENT 'Return on assets (%)',

    -- Dividends ----------------------------------------------------
    dividend_yield      DECIMAL(10,2)  COMMENT 'Dividend yield (%)',
    payout_ratio        DECIMAL(10,2)  COMMENT 'Dividend payout ratio (%)',

    -- Per-share data -----------------------------------------------
    book_per_share      DECIMAL(10,4)  COMMENT 'Book value per share',

    -- Revenue ------------------------------------------------------
    sales_musd          DECIMAL(18,2)  COMMENT 'Annual sales in millions USD',

    -- Calculated Graham metrics ------------------------------------
    graham_number       DECIMAL(12,4)  COMMENT 'sqrt(22.5 * EPS * BVPS); intrinsic value estimate',
    price_to_graham     DECIMAL(10,4)  COMMENT 'price / graham_number; <1 = potentially undervalued',
    graham_score        TINYINT        COMMENT 'How many Graham criteria passed (sector-adjusted, 0-7)',

    -- Metadata -----------------------------------------------------
    last_updated        TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,

    -- Constraints --------------------------------------------------
    PRIMARY KEY (ticker),
    INDEX idx_sector (sector),
    INDEX idx_industry (industry),
    INDEX idx_graham_score (graham_score),
    INDEX idx_pe_pb (pe_ratio, pb_ratio)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ----------------------------------------------------------------
-- 3. ETL log table - audit trail for every scraper run
-- ----------------------------------------------------------------
CREATE TABLE etl_log (
    id                  INT            AUTO_INCREMENT PRIMARY KEY,
    run_started         DATETIME       NOT NULL,
    run_finished        DATETIME,
    status              ENUM('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED')
                                       NOT NULL DEFAULT 'RUNNING',
    records_inserted    INT            DEFAULT 0,
    records_updated     INT            DEFAULT 0,
    records_failed      INT            DEFAULT 0,
    duration_seconds    INT,
    error_message       TEXT,
    notes               VARCHAR(500),

    INDEX idx_status (status),
    INDEX idx_run_started (run_started)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ----------------------------------------------------------------
-- 4. Power BI views - sector-adjusted Graham screens
-- ----------------------------------------------------------------

-- Classic Graham defensive screen (best fit: industrials, consumer defensive, utilities)
CREATE OR REPLACE VIEW vw_graham_defensive AS
SELECT *
FROM stock_fundamentals
WHERE pe_ratio > 0 AND pe_ratio < 15
  AND pb_ratio > 0 AND pb_ratio < 1.5
  AND current_ratio > 2
  AND eps_ttm > 0
  AND dividend_yield > 0
  AND market_cap_musd > 2000;

-- Bank-adjusted screen (skip current_ratio / debt metrics, focus on P/B & ROE)
CREATE OR REPLACE VIEW vw_banks_screen AS
SELECT *
FROM stock_fundamentals
WHERE industry LIKE '%Bank%'
  AND pe_ratio > 0 AND pe_ratio < 15
  AND pb_ratio > 0 AND pb_ratio < 1.5
  AND roe > 8
  AND dividend_yield > 0;

-- Cyclical-adjusted screen (auto, mining): looser current_ratio, stricter debt
CREATE OR REPLACE VIEW vw_cyclical_screen AS
SELECT *
FROM stock_fundamentals
WHERE (sector = 'Basic Materials' OR industry LIKE '%Auto%')
  AND pe_ratio > 0 AND pe_ratio < 15
  AND pb_ratio > 0 AND pb_ratio < 1.5
  AND current_ratio > 1.5
  AND debt_eq < 1.0
  AND eps_ttm > 0;

-- Sector summary for dashboard tiles
CREATE OR REPLACE VIEW vw_sector_summary AS
SELECT
    sector,
    COUNT(*)                       AS total_companies,
    SUM(CASE WHEN graham_score >= 5 THEN 1 ELSE 0 END) AS strong_graham_picks,
    AVG(pe_ratio)                  AS avg_pe,
    AVG(pb_ratio)                  AS avg_pb,
    AVG(dividend_yield)            AS avg_dividend_yield,
    MAX(last_updated)              AS most_recent_update
FROM stock_fundamentals
WHERE pe_ratio IS NOT NULL
GROUP BY sector;


-- ----------------------------------------------------------------
-- 5. Verification
-- ----------------------------------------------------------------
SHOW TABLES;
SELECT
    TABLE_NAME,
    TABLE_TYPE,
    TABLE_ROWS
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'graham_stocks';
