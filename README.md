# Graham Stocks Pipeline

> Automated daily ETL pipeline that ingests stock fundamentals from Finviz, applies Benjamin Graham's defensive investor criteria with **sector-aware adjustments**, and serves the results through MySQL and Power BI.

📖 *[Magyar verzió / Hungarian version](README_HU.md)*

---

## Project Goal

This project demonstrates a **production-grade analytics pipeline** for fundamental stock analysis. It was built as a portfolio piece during a career transition from premium banking (8 years) to a Business Analyst role — showcasing end-to-end data product capabilities: data engineering, methodology design, observability, and business intelligence delivery.

The system implements Benjamin Graham's classic *Defensive Investor* framework with a critical adaptation: criteria are **sector-adjusted**, because the original framework was designed for industrial companies and produces misleading results when applied uniformly to banks, mining companies, or other cyclical industries.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       Finviz (web)                          │
└──────────────────────────┬──────────────────────────────────┘
                           │ scrape via finvizfinance lib
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Python ETL Pipeline (scraper.py)               │
│  - Fetches Overview + Valuation + Financial views           │
│  - Cleans & transforms data (handles % parsing, etc.)       │
│  - Computes Graham Number, Price/Graham, Graham Score       │
│  - Applies sector-adjusted scoring logic                    │
└──────────────────────────┬──────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
  ┌─────────────┐  ┌─────────────────┐  ┌───────────┐
  │   stock_    │  │     stock_      │  │  etl_log  │
  │  fundament  │  │   fundamentals_ │  │  (audit)  │
  │     als     │  │      history    │  │           │
  │ (snapshot)  │  │  (time series)  │  │           │
  └──────┬──────┘  └────────┬────────┘  └─────┬─────┘
         │           7 SQL views              │
         └──────────────────┼─────────────────┘
                            ▼
                  ┌──────────────────┐         ┌──────────────────┐
                  │  Power BI        │ ◄────── │  Windows Task    │
                  │  Dashboard       │         │  Scheduler       │
                  │  (2 pages)       │         │  (daily 22:00)   │
                  └──────────────────┘         └──────────────────┘
```

### Tech Stack

- **MySQL 8.0** — Data warehouse on `localhost:3307`
- **Python 3.12** — ETL with `pandas`, `SQLAlchemy`, `finvizfinance`, `python-dotenv`
- **Windows Task Scheduler** — Daily automation via `run_scraper.bat`
- **Power BI Desktop** — Visualization layer (2-page interactive dashboard)

---

## Methodology: Graham Criteria with Sector Adjustments

The system scores each stock on a **0-7 scale** based on these criteria:

| # | Criterion | Default rule | Bank-specific override |
|---|---|---|---|
| 1 | Moderate P/E | P/E < 15 | same |
| 2 | Moderate P/B | P/B < 1.5 | same |
| 3 | Positive earnings | EPS > 0 | same |
| 4 | Pays dividend | Dividend yield > 0 | same |
| 5 | Adequate size | Market cap > $2B | same |
| 6 | Liquidity | Current Ratio > 2 | **ROE > 8%** |
| 7 | Low debt | LT Debt / Equity < 1 | **EPS growth (5y) > 0** |

**Why sector adjustments?** Graham himself excluded financial companies from his classic criteria because banks' balance sheets work fundamentally differently — what's a "liability" for an industrial firm is the core business for a bank. Mining and automotive companies are also highly cyclical, distorting trailing earnings. Applying the framework uniformly across sectors produces misleading results.

### Calculated Metrics

| Metric | Formula | Interpretation |
|---|---|---|
| **Graham Number** | √(22.5 × EPS × BVPS) | Intrinsic-value approximation; the maximum price you should pay if you want to satisfy P/E < 15 AND P/B < 1.5 simultaneously |
| **Price/Graham** | price / graham_number | < 1 = potentially undervalued by Graham's standard; ≥ 1 = at or above intrinsic value |
| **Graham Score** | count of passed criteria | Integer 0-7; used to rank stocks |

---

## Sector Coverage

15 Finviz industries across 6 sectors (~816 companies as of May 2026):

| Sector | Industries | Approx. count |
|---|---|---|
| **Financial** | Banks - Regional, Banks - Diversified | ~344 |
| **Consumer Cyclical** | Auto Manufacturers, Auto Parts | ~87 |
| **Basic Materials** | Gold, Silver, Copper, Other Industrial Metals & Mining | ~114 |
| **Consumer Defensive** | Beverages, Packaged Foods, Household & Personal Products | ~112 |
| **Utilities** | Regulated Electric, Regulated Gas | ~57 |
| **Industrials** | Construction Machinery, Specialty Industrial Machinery | ~102 |

---

## Repository Structure

```
graham-stocks-pipeline/
├── sql/
│   ├── 01_create_schema.sql              # Initial DB schema (3 tables, 4 views)
│   └── 04_add_history_table.sql          # Time-series history table + 3 views
├── scraper.py                            # Main ETL script
├── run_scraper.bat                       # Wrapper for Task Scheduler
├── requirements.txt                      # Python dependencies
├── .env.example                          # Config template (copy to .env)
├── .gitignore                            # Excludes secrets, venv, logs
├── graham_dashboard.pbix                 # Power BI report file
├── docs/
│   ├── executive_summary.png             # Dashboard screenshots
│   └── stock_picker.png
└── README.md
```

---

## Database Schema

### Tables

| Table | Purpose | Cardinality |
|---|---|---|
| `stock_fundamentals` | Current snapshot — one row per ticker | 1 row / ticker |
| `stock_fundamentals_history` | Daily time series (dual-write pattern) | 1 row / (ticker, date) |
| `etl_log` | Audit trail of every scraper run (status, counts, duration, errors) | 1 row / run |

### Views

**Snapshot screens** (on `stock_fundamentals`):
- `vw_graham_defensive` — classic Graham screen (best fit: industrials, staples, utilities)
- `vw_banks_screen` — bank-adjusted screen (skips current ratio, uses ROE)
- `vw_cyclical_screen` — looser current ratio, stricter debt for mining and auto
- `vw_sector_summary` — per-sector aggregates for dashboard tiles

**Time series** (on `stock_fundamentals_history`):
- `vw_sector_trend` — sector-level daily averages
- `vw_industry_trend` — industry-level daily averages (more granular)
- `vw_ticker_history` — per-ticker evolution for drill-down charts

---

## Setup Instructions

### Prerequisites

- Windows 10/11 (Linux/macOS compatible with minor changes)
- MySQL Server 8.0+ running on port `3307`
- Python 3.12+
- Power BI Desktop (free)
- MySQL Connector/NET (required for Power BI ↔ MySQL connection)

### Steps

1. **Database setup**
   - Open MySQL Workbench, connect to your local instance.
   - Run `sql/01_create_schema.sql`, then `sql/04_add_history_table.sql`.
   - Create a user `graham_user` with `ALL PRIVILEGES` on `graham_stocks` database.

2. **Python environment**
   ```powershell
   cd graham-stocks-pipeline
   python -m venv venv
   venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

3. **Configuration**
   - Copy `.env.example` → `.env`
   - Edit `.env` with your MySQL password
   - **Never commit `.env`** (it's in `.gitignore`)

4. **First run** (test mode — only 2 industries)
   ```powershell
   # In .env: TEST_MODE=1
   python scraper.py
   ```

5. **Production run**
   ```powershell
   # In .env: TEST_MODE=0
   python scraper.py
   ```

6. **Daily automation** (Windows): Set up Task Scheduler to run `run_scraper.bat` daily at 22:00. The wrapper handles venv activation, logs to `scheduler.log`, and returns proper exit codes.

7. **Power BI**: Open `graham_dashboard.pbix`. Refresh the data source if prompted (it queries `localhost:3307/graham_stocks` via the MySQL Connector/NET).

---

## Key Findings

After the first production scrapes (May 2026), the dataset revealed several insights:

1. **No stock currently passes all 7 criteria.** The maximum observed Graham Score is **6** — meaning even the best candidates fail at least one criterion. This validates Graham's own observation that the perfect value stock is rare.

2. **Sector hit-rates differ substantially.** While 290 stocks (35%) meet ≥5 criteria, the proportion varies sharply:
   - **Financials**: ~58% hit-rate (banks often have low P/E and P/B)
   - **Consumer Defensive**: ~27% hit-rate (staples are currently expensive)

3. **The classic Graham paradigm appears inverted in 2026.** The "defensive favorites" (consumer staples, utilities) are *not* the cheapest sectors. Financials and certain mining sub-sectors are producing better value-screen results.

4. **Data drift detected on day 2.** The history table caught one ticker disappearing from the Finviz screen between consecutive scrapes — the kind of micro-event a snapshot-only system would silently miss.

5. **Sample value-trap detected.** A small bank (Hoyne Bancorp, HYNE) traded at P/B 0.78 (below book value) but had P/E 554 due to near-zero earnings — a textbook value trap. The Graham Score caught this by only awarding 2-3 points.

---

## Dashboard

### Page 1 — Executive Summary

- 4 KPI cards: Total Companies, Total Sectors, Strong Picks (≥5 score), Avg Graham Score
- Donut chart: Companies by Sector
- Bar chart: Strong Graham Picks by Sector

![Executive Summary](docs/executive_summary.png)

### Page 2 — Stock Picker

- Interactive slicers: Graham Score, Sector, Min Market Cap
- Sortable table: all ~816 stocks with fundamentals and computed Graham metrics
- Cross-page sync: slicer selections also filter the Executive Summary visuals

![Stock Picker](docs/stock_picker.png)

---

## Future Improvements

- **Anomaly detection**: alert when a ticker's day-over-day change exceeds N standard deviations
- **Email digest**: daily summary of new strong picks delivered to inbox
- **Backtesting module**: simulate "what if I had bought top picks 6 months ago"
- **Bank-specific extensions**: Tier 1 capital ratio, Net Interest Margin (would require alternative data source — Finviz doesn't surface these)
- **Cloud deployment**: containerize with Docker, schedule via Airflow on a cloud VM

---

## Development Process & AI Tooling

This project was developed in collaboration with **Claude (Anthropic's AI assistant)** as a pair-programming partner. AI-assisted development is a standard part of professional software work in 2026, and I'd rather be transparent about it than pretend otherwise. Here's the honest split:

**My contributions (conceptual & analytical):**
- **Methodology design** — sector selection, sector-specific Graham adjustments (why banks need different rules than industrials), the 0–7 scoring system
- **Domain interpretation** — leveraging 8 years of banking experience to recognize why classical Graham criteria fail on financials and how to adapt them
- **Architectural decisions** — choosing the dual-write pattern, designing the audit-log observability, deciding snapshot vs. time-series trade-offs
- **Quality control** — spotting the percentage-vs-decimal parsing bug, identifying the HYNE value-trap example, validating the max Graham score finding directly in SQL

**AI assistance accelerated:**
- Boilerplate generation (SQL DDL, Python parsers, batch wrappers)
- Code structure, idiomatic patterns, and best practices
- Debugging support and stepwise explanations
- This documentation itself

The aim was not to prove I can write every line of code in a vacuum — it was to demonstrate that I can **design, build, debug, and ship a complete data product**, while collaborating effectively with modern tools. That's the role of a Business Analyst in 2026.

---

## About the Author

This project was built by **Tamás Sturcz** during a career transition from premium banking (8 years at a Hungarian bank) toward Business Analyst roles. The goal was to combine financial domain expertise with modern data tooling — to deliver analysis that is technically rigorous *and* commercially relevant.

📧 tamassturcz@gmail.com

---

## License

MIT — fork it, adapt it, learn from it.
