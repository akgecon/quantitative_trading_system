# Quantitative Trading System - Operations Guide

## Overview

This trading system generates daily stock positions using technical factors and machine learning models. The system calculates trading signals based on market data through the previous trading day and outputs position weights for execution.

## System Architecture

The system consists of two main components:
1. **EOD Price Update System** (`R/02_eod_update.R`) - Fetches daily price data from Tiingo
2. **Strategy Update System** (`R/complete_daily_system_adjusted.R`) - Generates trading signals

All state is automatically persisted in RDS files - no manual saving required.

## Key Concepts

### Data Flow Timeline

The system operates on a T-1 basis:
- **Data Date (T-1)**: The date through which market data is used
- **Run Date (T)**: When you run the system  
- **Execution Window**: End of day T or morning of T+1
- **Return Measurement**: One day after execution

### File Naming Convention

Files are dated with the **data date** (last date of data used), not the run date.

**Example: Running on Wednesday July 10, 2025**
- Uses data through: Tuesday July 9, 2025 close
- Generates file: `aggregate_weights_20250709.csv`
- The "20250709" represents Tuesday's date (the data date)
- Execution options:
  - Wednesday July 10 end-of-day  
  - Thursday July 11 morning
- Returns measured: One day after execution

## Daily Update Instructions

### Prerequisites

Ensure these are configured:
- `RIINGO_TOKEN` environment variable set
- `config/universe.csv` file with stock tickers
- Initial backtest has been run (RDS files exist in `data/`)

### When to Run

**Critical**: Only run AFTER market close when EOD data is available.

**Optimal run times**:
- **Evening**: 5:00 PM - 11:00 PM ET (same day data, prepare for next day)
- **Morning**: 6:00 AM - 9:00 AM ET (previous day's data, time to review)
- **Never**: During market hours (9:30 AM - 4:00 PM ET)

### How to Run Daily Update

```r
# Set working directory
setwd("~/quantitative_trading_system")

# Source and run the complete daily system
source("R/complete_daily_system_adjusted.R")
result <- run_complete_daily_update()
```

That's it! The system automatically:
- Updates the price database
- Calculates new trading signals
- Exports weight files
- Saves state to RDS

### Market Hours Check (Optional)

To prevent running during market hours:

```r
# Check if market has closed
market_close_et <- as.POSIXct(paste(Sys.Date(), "16:30:00"), tz = "America/New_York")
current_time_et <- with_tz(Sys.time(), "America/New_York")

if (current_time_et < market_close_et) {
  wait_hours <- round(as.numeric(difftime(market_close_et, current_time_et, units = "hours")), 1)
  cat("⏰ Market hasn't closed yet. Please wait", wait_hours, "hours.\n")
  cat("Run again after 5:00 PM ET\n")
} else {
  source("R/complete_daily_system_adjusted.R")
  result <- run_complete_daily_update()
}
```

## Output Files

### Location
All outputs are saved to the `output/` directory.

### Key Files Generated

#### 1. aggregate_weights_YYYYMMDD.csv
Daily position weights for trading:
```csv
ticker,aggregate_weight,date,long_short,weight_magnitude
BDX,0.0500,2025-07-08,Long,Large (≥5%)
TMO,0.0421,2025-07-08,Long,Medium (2-5%)
TD,-0.0500,2025-07-08,Short,Large (≥5%)
MSFT,-0.0358,2025-07-08,Short,Small (1-2%)
```

#### 2. portfolio_summary_YYYYMMDD.csv
Portfolio-level metrics:
```csv
date,total_positions,long_exposure,short_exposure,net_exposure,gross_exposure,largest_position,long_positions,short_positions
2025-07-08,59,0.4841,-0.4841,0.0000,0.9682,0.0500,30,29
```

#### 3. complete_aggregate_weights_timeseries.csv
Historical record of all daily weights

#### 4. weights_with_adjusted_returns_validation.parquet
Historical weights with realized returns for performance analysis

## Understanding the Date Labels

The date in the filename represents the "as of" date for the data:

**Example: File named `aggregate_weights_20250709.csv`**
- Contains signals calculated using data through July 9 close
- Generated when you run the system on July 10 (morning or evening)
- Ready for execution on July 10 (end of day) or July 11 (morning)

This ensures reproducibility - the same data always produces the same signals regardless of when you run the system.

## System State Management

### Automatic Persistence

The system maintains its state in two RDS files:
- `data/enhanced_slice_results_baseline.rds` - Original baseline (never modified)
- `data/enhanced_slice_results_current_adjusted.rds` - Current state (auto-updated daily)

**You never need to manually save or load these files** - the daily update handles everything.

### State Verification

To check your current system state:
```r
# Check latest processed date
state <- readRDS("data/enhanced_slice_results_current_adjusted.rds")
cat("System current through:", max(state$daily_results$date), "\n")
cat("Total trading days:", nrow(state$daily_results), "\n")
```

## Troubleshooting

### Common Issues and Solutions

#### 1. "All tickers failed to download any data"
- **Cause**: Running before market close (EOD data not available)
- **Solution**: Wait until after 5 PM ET and run again

#### 2. "Operational implementation not found"
- **Cause**: Missing required R scripts
- **Solution**: Ensure all files in `R/` directory are present

#### 3. "Config file config.yml not found"
- **Cause**: The `config` package looking for config file
- **Solution**: Create empty file: `cat("default:\n  dummy: true\n", file = "config.yml")`

#### 4. "No existing backtest results found"
- **Cause**: Initial backtest hasn't been run
- **Solution**: Run initialization scripts (see Initial Setup)

### Validation After Each Run

Check for these success indicators:
- Console shows "✅ UPDATE FINISHED"
- New CSV files in `output/` with yesterday's date
- Portfolio metrics displayed with ~0% net exposure
- Performance statistics shown (return and Sharpe ratio)

## Daily Trading Workflow

### Morning Routine
1. Review weights from yesterday's update
2. Check portfolio metrics and position counts
3. Execute trades from `aggregate_weights_YYYYMMDD.csv`

### Evening Routine
1. After 5 PM ET, run the daily update
2. Review new weights for tomorrow
3. Check performance metrics in console output

## Example Daily Session

```r
# July 10, 2025 at 6:00 PM ET
setwd("~/quantitative_trading_system")
source("R/complete_daily_system_adjusted.R")
result <- run_complete_daily_update()

# Output will show:
# ✅ Database updated (if new data available)
# ✅ Strategy processed through July 10
# ✅ Files exported: aggregate_weights_20250710.csv
# ✅ Performance: X% return, Y Sharpe ratio
# ✅ Ready for July 11 trading
```

## Performance Monitoring

The system displays recent performance with each update:
- 30-day annualized return
- 30-day annualized Sharpe ratio
- Portfolio exposure metrics

## Emergency Procedures

### If Update Fails
1. Check console for specific error messages
2. Verify market has closed (after 5 PM ET)
3. Try running again in 30 minutes
4. Use previous day's weights if necessary

### System Recovery
If RDS file becomes corrupted:
```r
# Restore from baseline
file.copy("data/enhanced_slice_results_baseline.rds", 
          "data/enhanced_slice_results_current_adjusted.rds", 
          overwrite = TRUE)
# Then run updates to catch up to current date
```

## Initial Setup (First Time Only)

If starting from scratch:
```r
# Run model training
source('R/08_exact_forecast_returns.R')

# Run initial backtest
source('R/09_exact_slice_portfolio.R')

# Save baseline
saveRDS(enhanced_slice_results_final, "data/enhanced_slice_results_baseline.rds")

# Then run daily update
source("R/complete_daily_system_adjusted.R")
result <- run_complete_daily_update()
```

## Best Practices

1. **Consistency**: Run at the same time each day
2. **Patience**: Wait for market close before running
3. **Monitoring**: Review performance metrics daily
4. **Backup**: System maintains automatic state backups
5. **Simplicity**: Just run one command - system handles the rest

---

*Version: 3.0 - Production System with Adjusted Returns*
*Last Updated: July 2025*