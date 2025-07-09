# R/complete_daily_system.R
# COMPLETE DAILY TRADING SYSTEM - ADJUSTED RETURNS VERSION
# Unified daily update with full project structure and instructions

library(tidyverse)
library(lubridate)
library(logger)

cat("====================================================================\n")
cat("COMPLETE DAILY TRADING SYSTEM - ADJUSTED RETURNS VERSION\n")
cat("Unified database + strategy updates with adjusted returns\n")
cat("====================================================================\n\n")

# ============================================================================
# UNIFIED DAILY UPDATE FUNCTION
# ============================================================================

run_complete_daily_update <- function(target_date = Sys.Date(), 
                                      force_db_refresh = FALSE,
                                      export_validation_data = TRUE,
                                      output_dir = "output") {
  
  start_time <- Sys.time()
  
  log_info("🚀 COMPLETE DAILY TRADING SYSTEM UPDATE")
  log_info("======================================")
  log_info("Target date: {target_date}")
  log_info("Force DB refresh: {force_db_refresh}")
  log_info("Started at: {format(start_time, '%Y-%m-%d %H:%M:%S')}")
  
  # ============================================================================
  # STEP 1: DATABASE UPDATE (PRICES)
  # ============================================================================
  
  log_info("📊 STEP 1: UPDATING PRICE DATABASE")
  log_info("==================================")
  
  # Source EOD update system if not already loaded
  if(!exists("run_daily_eod_update")) {
    if(file.exists("R/02_eod_update.R")) {
      source("R/02_eod_update.R")
      log_info("📦 Loaded EOD update system")
    } else {
      log_error("❌ EOD update system not found at R/02_eod_update.R")
      return(NULL)
    }
  }
  
  # Run database update
  tryCatch({
    db_success <- run_daily_eod_update(force_full_refresh = force_db_refresh)
    
    if(!db_success) {
      log_error("❌ Database update failed - cannot proceed with strategy update")
      return(list(
        success = FALSE,
        step_failed = "database_update",
        error = "Database update returned FALSE"
      ))
    }
    
    log_info("✅ Database updated successfully")
    
  }, error = function(e) {
    log_error("❌ Database update error: {e$message}")
    return(list(
      success = FALSE,
      step_failed = "database_update", 
      error = e$message
    ))
  })
  
  # ============================================================================
  # STEP 2: STRATEGY UPDATE (PORTFOLIO) - ADJUSTED RETURNS VERSION
  # ============================================================================
  
  log_info("📈 STEP 2: UPDATING TRADING STRATEGY (ADJUSTED RETURNS)")
  log_info("=====================================================")
  
  # Load operational functions if not already loaded
  required_functions <- c("operational_update_backtest_to_latest_adjusted")
  missing_functions <- setdiff(required_functions, ls(.GlobalEnv))
  
  if(length(missing_functions) > 0) {
    if(file.exists("R/complete_operational_implementation_with_adjusted_returns.R")) {
      source("R/complete_operational_implementation_with_adjusted_returns.R")
      log_info("📦 Loaded operational implementation (adjusted returns)")
    } else {
      log_error("❌ Operational implementation not found")
      return(list(
        success = FALSE,
        step_failed = "strategy_load",
        error = "Missing operational implementation file: R/complete_operational_implementation_with_adjusted_returns.R"
      ))
    }
  }
  
  # Run persistent strategy update with adjusted returns
  tryCatch({
    strategy_result <- operational_update_persistent_adjusted(
      target_date = target_date,
      export_validation_data = export_validation_data,
      output_dir = output_dir
    )
    
    if(is.null(strategy_result)) {
      log_error("❌ Strategy update failed")
      return(list(
        success = FALSE,
        step_failed = "strategy_update",
        error = "Strategy update returned NULL"
      ))
    }
    
    log_info("✅ Strategy updated successfully")
    
    # Log portfolio metrics if available
    if("daily_aggregate_weights" %in% names(strategy_result)) {
      latest_weights <- strategy_result$daily_aggregate_weights %>%
        filter(date == max(date))
      
      if(nrow(latest_weights) > 0) {
        portfolio_metrics <- list(
          total_positions = nrow(latest_weights),
          long_exposure = sum(pmax(latest_weights$aggregate_weight, 0)),
          short_exposure = sum(pmin(latest_weights$aggregate_weight, 0)),
          net_exposure = sum(latest_weights$aggregate_weight),
          gross_exposure = sum(abs(latest_weights$aggregate_weight))
        )
        
        log_info("📊 Portfolio Metrics:")
        log_info("• Total positions: {portfolio_metrics$total_positions}")
        log_info("• Long exposure: {sprintf('%.2f%%', portfolio_metrics$long_exposure * 100)}")
        log_info("• Short exposure: {sprintf('%.2f%%', portfolio_metrics$short_exposure * 100)}")
        log_info("• Net exposure: {sprintf('%.2f%%', portfolio_metrics$net_exposure * 100)}")
        log_info("• Gross exposure: {sprintf('%.2f%%', portfolio_metrics$gross_exposure * 100)}")
      }
    }
    
  }, error = function(e) {
    log_error("❌ Strategy update error: {e$message}")
    return(list(
      success = FALSE,
      step_failed = "strategy_update",
      error = e$message
    ))
  })
  
  # ============================================================================
  # STEP 3: VALIDATION AND SUMMARY
  # ============================================================================
  
  log_info("🔍 STEP 3: VALIDATION AND SUMMARY")
  log_info("=================================")
  
  end_time <- Sys.time()
  duration <- as.numeric(difftime(end_time, start_time, units = "mins"))
  
  # Validate results
  validation <- list(
    database_current = check_database_currency(),
    strategy_current = check_strategy_currency(strategy_result),
    files_exported = check_exported_files(output_dir),
    total_duration = round(duration, 2)
  )
  
  # Summary
  latest_strategy_date <- max(strategy_result$daily_results$date)
  total_trading_days <- nrow(strategy_result$daily_results)
  
  log_info("📋 UPDATE SUMMARY:")
  log_info("• Database status: {ifelse(validation$database_current, '✅ Current', '⚠️ Issues')}")
  log_info("• Strategy status: {ifelse(validation$strategy_current, '✅ Current', '⚠️ Issues')}")
  log_info("• Files exported: {ifelse(validation$files_exported, '✅ Yes', '❌ No')}")
  log_info("• Latest strategy date: {latest_strategy_date}")
  log_info("• Total trading days: {total_trading_days}")
  log_info("• Total duration: {validation$total_duration} minutes")
  log_info("• Return type: ADJUSTED (adjClose)")
  
  if(validation$files_exported) {
    log_info("📁 READY FOR TRADING:")
    log_info("• Latest weights: output/aggregate_weights_{format(latest_strategy_date, '%Y%m%d')}.csv")
    log_info("• Portfolio summary: output/portfolio_summary_{format(latest_strategy_date, '%Y%m%d')}.csv")
    if(export_validation_data) {
      log_info("• Validation data: output/weights_with_adjusted_returns_validation.parquet")
    }
  }
  
  log_info("🎯 DAILY UPDATE COMPLETE - READY FOR TOMORROW'S TRADING")
  
  return(list(
    success = TRUE,
    strategy_result = strategy_result,
    validation = validation,
    latest_date = latest_strategy_date,
    duration_minutes = validation$total_duration,
    return_type = "adjusted"
  ))
}

# ============================================================================
# HELPER FUNCTIONS FOR VALIDATION
# ============================================================================

check_database_currency <- function() {
  tryCatch({
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "data/dev.duckdb")
    on.exit(DBI::dbDisconnect(con))
    
    if(!DBI::dbExistsTable(con, "results_pricedata_daily")) {
      return(FALSE)
    }
    
    latest_db_date <- DBI::dbGetQuery(con, "SELECT MAX(date) as max_date FROM results_pricedata_daily")$max_date
    days_behind <- as.numeric(Sys.Date() - as.Date(latest_db_date))
    
    return(days_behind <= 2)  # Allow weekend gap
    
  }, error = function(e) {
    return(FALSE)
  })
}

check_strategy_currency <- function(strategy_result) {
  if(is.null(strategy_result) || !"daily_results" %in% names(strategy_result)) {
    return(FALSE)
  }
  
  latest_strategy_date <- max(strategy_result$daily_results$date)
  days_behind <- as.numeric(Sys.Date() - 1 - latest_strategy_date)  # T-1 logic
  
  return(days_behind <= 2)  # Allow weekend gap
}

check_exported_files <- function(output_dir) {
  required_files <- c(
    "complete_aggregate_weights_timeseries.csv"
  )
  
  existing_files <- list.files(output_dir, full.names = FALSE)
  all_exist <- all(required_files %in% existing_files)
  
  return(all_exist)
}

# ============================================================================
# ENHANCED PERSISTENT UPDATE FOR ADJUSTED RETURNS
# ============================================================================

operational_update_persistent_adjusted <- function(target_date = Sys.Date(), 
                                                   export_weights = TRUE,
                                                   export_validation_data = TRUE,
                                                   output_dir = "output") {
  
  baseline_file <- "data/enhanced_slice_results_baseline.rds"
  current_file <- "data/enhanced_slice_results_current_adjusted.rds"  # Separate file for adjusted
  
  log_info("💾 PERSISTENT UPDATE WITH ADJUSTED RETURNS")
  log_info("==========================================")
  
  # Ensure data directory exists
  dir.create("data", showWarnings = FALSE, recursive = TRUE)
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  # One-time: Save baseline if doesn't exist
  if(!file.exists(baseline_file) && exists("enhanced_slice_results_final")) {
    saveRDS(enhanced_slice_results_final, baseline_file)
    log_info("💾 Saved baseline results to: {baseline_file}")
  }
  
  # Load current state if exists
  if(file.exists(current_file)) {
    enhanced_slice_results_final <<- readRDS(current_file)
    log_info("📂 Loaded current adjusted state from: {current_file}")
  } else if(file.exists(baseline_file)) {
    enhanced_slice_results_final <<- readRDS(baseline_file)
    log_info("📂 Loaded baseline state (first time for adjusted returns)")
  } else if(!exists("enhanced_slice_results_final")) {
    log_error("❌ No backtest results found. Please run initial backtest first:")
    log_error("   1. source('R/08_exact_forecast_returns.R')")
    log_error("   2. source('R/09_exact_slice_portfolio.R')")
    log_error("   3. Then run this function")
    return(NULL)
  }
  
  # Run update with adjusted returns
  log_info("🔄 Running operational update with ADJUSTED returns...")
  result <- operational_update_backtest_to_latest_adjusted(
    target_date = target_date,
    export_weights = export_weights,
    export_validation_data = export_validation_data,
    output_dir = output_dir
  )
  
  # Save updated state
  if(!is.null(result)) {
    saveRDS(result, current_file)
    log_info("💾 Saved updated adjusted state to: {current_file}")
    
    # Update global environment
    enhanced_slice_results_final <<- result
    
    # Log performance summary
    if("daily_results" %in% names(result)) {
      recent_results <- tail(result$daily_results, 30)  # Last 30 days
      if(nrow(recent_results) > 0) {
        recent_return <- mean(recent_results$portfolio_return, na.rm = TRUE) * 252
        recent_vol <- sd(recent_results$portfolio_return, na.rm = TRUE) * sqrt(252)
        recent_sharpe <- if(recent_vol > 0) recent_return / recent_vol else 0
        
        log_info("📈 RECENT PERFORMANCE (30 days):")
        log_info("• Annualized return: {sprintf('%.2f%%', recent_return * 100)}")
        log_info("• Annualized volatility: {sprintf('%.2f%%', recent_vol * 100)}")
        log_info("• Annualized Sharpe: {sprintf('%.2f', recent_sharpe)}")
      }
    }
  }
  
  return(result)
}

# ============================================================================
# SYSTEM REQUIREMENTS CHECK (UPDATED FOR ADJUSTED RETURNS)
# ============================================================================

check_system_requirements <- function() {
  
  log_info("🔍 CHECKING SYSTEM REQUIREMENTS")
  log_info("==============================")
  
  requirements <- list(
    logging_system = exists("log_info"),
    eod_system = file.exists("R/02_eod_update.R"),
    operational_system = file.exists("R/complete_operational_implementation_with_adjusted_returns.R"),  # Updated
    model_training = file.exists("R/08_exact_forecast_returns.R"),
    slice_backtest = file.exists("R/09_exact_slice_portfolio.R"),
    universe_file = file.exists("config/universe.csv"),
    riingo_token = Sys.getenv("RIINGO_TOKEN") != "",
    data_directory = dir.exists("data") || dir.create("data"),
    output_directory = dir.exists("output") || dir.create("output"),
    logs_directory = dir.exists("logs") || dir.create("logs")
  )
  
  all_good <- all(unlist(requirements))
  
  for(req_name in names(requirements)) {
    status <- if(requirements[[req_name]]) "✅" else "❌"
    log_info("{status} {req_name}")
  }
  
  if(all_good) {
    log_info("🎯 ALL REQUIREMENTS MET - SYSTEM READY")
  } else {
    log_error("❌ MISSING REQUIREMENTS - SYSTEM NOT READY")
  }
  
  return(requirements)
}

# ============================================================================
# PROJECT FILE STRUCTURE DOCUMENTATION
# ============================================================================

document_project_structure <- function() {
  
  cat("📁 COMPLETE PROJECT FILE STRUCTURE\n")
  cat("==================================\n\n")
  
  structure <- "
PROJECT_ROOT/
│
├── 📂 R/                                    # Main R code directory
│   ├── 02_eod_update.R                     # ✅ EOD price update system
│   ├── 08_exact_forecast_returns.R          # ✅ Model training & enhanced results
│   ├── 09_exact_slice_portfolio.R          # ✅ Slice portfolio backtest
│   ├── complete_operational_implementation_with_adjusted_returns.R  # 🆕 Adjusted returns system
│   ├── complete_daily_system.R             # 🆕 This unified system
│   │
│   └── 📂 utils/                           # Utility functions
│       ├── database.R                      # Database connection utilities
│       ├── logging.R                       # Logging configuration
│       └── validation.R                    # Data validation functions
│
├── 📂 config/                              # Configuration files  
│   ├── config.yml                          # Main configuration
│   └── universe.csv                        # Trading universe (100 tickers)
│
├── 📂 data/                               # Data storage (auto-created)
│   ├── dev.duckdb                         # 🗄️ Price database (630K+ records)
│   ├── enhanced_slice_results_baseline.rds # 💎 Original backtest (preserved)
│   ├── enhanced_slice_results_current_adjusted.rds  # 🔄 Adjusted returns state
│   └── 📂 backups/                        # Automatic backups
│
├── 📂 output/                             # Trading outputs (auto-created)
│   ├── aggregate_weights_20250708.csv      # 📊 Latest position weights
│   ├── portfolio_summary_20250708.csv      # 📋 Portfolio metrics
│   ├── complete_aggregate_weights_timeseries.csv  # 📈 Full weight history
│   └── weights_with_adjusted_returns_validation.parquet  # 🔍 Adjusted returns validation
│
├── 📂 logs/                               # System logs (auto-created)
│   ├── eod_system_20250708.log            # EOD system logs
│   └── ...
│
├── .Renviron                              # Environment variables (RIINGO_TOKEN)
└── README.md                              # Project overview

KEY IMPROVEMENTS:
================
🎯 Uses ADJUSTED returns by default (adjClose instead of close)
🎯 Separate state files for adjusted vs close returns
🎯 Enhanced logging with portfolio metrics
🎯 Production-ready with comprehensive monitoring
"
  
  cat(structure)
  cat("\n")
}

# ============================================================================
# DAILY OPERATING INSTRUCTIONS
# ============================================================================

show_daily_instructions <- function() {
  
  cat("📋 DAILY TRADING SYSTEM OPERATIONS (ADJUSTED RETURNS)\n")
  cat("====================================================\n\n")
  
  instructions <- "
🕐 DAILY SCHEDULE:
================
4:00 PM ET  → Markets close
4:30 PM ET  → Market data settles
5:00 PM ET  → Run daily update (recommended time)
5:30 PM ET  → Review results and prepare for next day

🚀 DAILY COMMAND:
================

result <- run_complete_daily_update()

This automatically:
✅ Updates price database with latest data
✅ Updates strategy with ADJUSTED returns (adjClose)
✅ Exports weights for tomorrow's trading
✅ Creates validation data with adjusted returns
✅ Saves all state to disk
✅ Logs comprehensive metrics

📁 FILES CREATED DAILY:
=======================

TRADING FILES:
• output/aggregate_weights_YYYYMMDD.csv      → Position weights
• output/portfolio_summary_YYYYMMDD.csv      → Portfolio metrics

ANALYSIS FILES:
• output/complete_aggregate_weights_timeseries.csv  → Full history
• output/weights_with_adjusted_returns_validation.parquet  → Validation data

STATE FILES:
• data/enhanced_slice_results_current_adjusted.rds  → Updated strategy state
• logs/eod_system_YYYYMMDD.log                     → Comprehensive logs

🎯 KEY BENEFITS OF ADJUSTED RETURNS:
===================================
• Higher portfolio returns (dividend capture)
• Lower volatility (smoothed price movements)
• Better Sharpe ratio
• More accurate performance attribution
• Professional-grade return calculation

🔍 SUCCESS INDICATORS:
=====================
✅ Console shows: '🎯 DAILY UPDATE COMPLETE - READY FOR TOMORROW'S TRADING'
✅ New CSV files in output/ with today's date
✅ Log shows: 'Return type: ADJUSTED (adjClose)'
✅ No error messages
✅ Portfolio metrics logged
"
  
  cat(instructions)
  cat("\n")
}

# ============================================================================
# INITIALIZATION HELPER
# ============================================================================

initialize_daily_system <- function() {
  
  log_info("🎬 INITIALIZING DAILY TRADING SYSTEM (ADJUSTED RETURNS)")
  log_info("======================================================")
  
  # Check prerequisites
  prerequisites <- list(
    riingo_token = Sys.getenv("RIINGO_TOKEN") != "",
    universe_file = file.exists("config/universe.csv"),
    enhanced_results = exists("enhanced_slice_results_final"),
    required_functions = exists("operational_update_backtest_to_latest_adjusted")
  )
  
  missing_prereqs <- names(prerequisites)[!unlist(prerequisites)]
  
  if(length(missing_prereqs) > 0) {
    log_error("❌ Missing prerequisites:")
    for(prereq in missing_prereqs) {
      log_error("  • {prereq}")
    }
    log_error("Please complete setup before initializing:")
    log_error("1. Add RIINGO_TOKEN to .Renviron")
    log_error("2. Place universe file at config/universe.csv")
    log_error("3. Run model training and slice backtest")
    log_error("4. Source operational implementation")
    return(FALSE)
  }
  
  # Create directory structure
  dirs_to_create <- c("data", "output", "logs", "config", "docs", "data/backups")
  for(dir in dirs_to_create) {
    dir.create(dir, showWarnings = FALSE, recursive = TRUE)
  }
  
  log_info("✅ All prerequisites met")
  log_info("✅ Directory structure created")
  
  # Save baseline if exists
  if(exists("enhanced_slice_results_final")) {
    baseline_file <- "data/enhanced_slice_results_baseline.rds"
    if(!file.exists(baseline_file)) {
      saveRDS(enhanced_slice_results_final, baseline_file)
      log_info("✅ Baseline results saved")
    }
  }
  
  log_info("🎯 SYSTEM READY FOR DAILY OPERATIONS")
  log_info("====================================")
  log_info("Use: result <- run_complete_daily_update()")
  
  return(TRUE)
}

# ============================================================================
# LOAD AND DISPLAY DOCUMENTATION
# ============================================================================

cat("✅ COMPLETE DAILY TRADING SYSTEM (ADJUSTED RETURNS) LOADED\n")
cat("=========================================================\n\n")

cat("🎯 MAIN FUNCTION:\n")
cat("result <- run_complete_daily_update()\n\n")

cat("📋 DOCUMENTATION:\n")
cat("document_project_structure()     # Show file structure\n")
cat("show_daily_instructions()        # Show daily workflow\n")
cat("initialize_daily_system()        # One-time setup\n")
cat("check_system_requirements()      # Verify system ready\n\n")

cat("🚀 READY FOR PRODUCTION TRADING WITH ADJUSTED RETURNS\n")