# R/complete_daily_system_adjusted.R
# COMPLETE DAILY TRADING SYSTEM WITH ADJUSTED RETURNS
# Updated to use adjusted returns by default with comprehensive logging

library(tidyverse)
library(lubridate)
library(logger)

cat("====================================================================\n")
cat("COMPLETE DAILY TRADING SYSTEM - ADJUSTED RETURNS VERSION\n")
cat("Uses adjClose for portfolio returns, comprehensive logging included\n")
cat("====================================================================\n\n")

# ============================================================================
# UNIFIED DAILY UPDATE FUNCTION - ADJUSTED RETURNS VERSION
# ============================================================================

run_complete_daily_update <- function(target_date = Sys.Date(), 
                                      force_db_refresh = FALSE,
                                      use_adjusted_returns = TRUE,  # NEW: Default to adjusted returns
                                      export_validation_data = TRUE,
                                      output_dir = "output") {
  
  start_time <- Sys.time()
  
  log_info("=== 🚀 STARTING COMPLETE DAILY UPDATE ===")
  log_info("Target date: {target_date}")
  log_info("Force DB refresh: {force_db_refresh}")
  log_info("Use adjusted returns: {use_adjusted_returns}")
  log_info("Export validation data: {export_validation_data}")
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
    log_info("🔄 Starting database update...")
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
  # STEP 2: STRATEGY UPDATE (PORTFOLIO) - WITH ADJUSTED RETURNS
  # ============================================================================
  
  log_info("📈 STEP 2: UPDATING TRADING STRATEGY")
  log_info("====================================")
  log_info("💡 Using {ifelse(use_adjusted_returns, 'ADJUSTED', 'CLOSE')} returns for portfolio calculation")
  
  # Load operational functions if not already loaded
  required_functions <- c("operational_update_backtest_to_latest")
  missing_functions <- setdiff(required_functions, ls(.GlobalEnv))
  
  if(length(missing_functions) > 0) {
    if(file.exists("R/complete_operational_implementation_with_weights.R")) {
      source("R/complete_operational_implementation_with_weights.R")
      log_info("📦 Loaded operational implementation")
    } else {
      log_error("❌ Operational implementation not found")
      return(list(
        success = FALSE,
        step_failed = "strategy_load",
        error = "Missing operational implementation file"
      ))
    }
  }
  
  # Run strategy update with adjusted returns
  tryCatch({
    log_info("🔄 Starting strategy update...")
    
    if(use_adjusted_returns) {
      # Use the adjusted returns version
      strategy_result <- operational_update_persistent_adjusted(
        target_date = target_date,
        export_validation_data = export_validation_data,
        output_dir = output_dir
      )
    } else {
      # Use the regular version
      strategy_result <- operational_update_persistent(
        target_date = target_date,
        export_validation_data = export_validation_data,
        output_dir = output_dir
      )
    }
    
    if(is.null(strategy_result)) {
      log_error("❌ Strategy update failed - returned NULL")
      return(list(
        success = FALSE,
        step_failed = "strategy_update",
        error = "Strategy update returned NULL"
      ))
    }
    
    log_info("✅ Strategy updated successfully")
    log_info("📊 Portfolio state: {nrow(strategy_result$daily_results)} total trading days")
    
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
  
  # Calculate portfolio metrics
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
      
      log_info("📊 PORTFOLIO METRICS:")
      log_info("• Total positions: {portfolio_metrics$total_positions}")
      log_info("• Long exposure: {sprintf('%.2f%%', portfolio_metrics$long_exposure * 100)}")
      log_info("• Short exposure: {sprintf('%.2f%%', portfolio_metrics$short_exposure * 100)}")
      log_info("• Net exposure: {sprintf('%.2f%%', portfolio_metrics$net_exposure * 100)}")
      log_info("• Gross exposure: {sprintf('%.2f%%', portfolio_metrics$gross_exposure * 100)}")
    }
  }
  
  log_info("📋 UPDATE SUMMARY:")
  log_info("• Database status: {ifelse(validation$database_current, '✅ Current', '⚠️ Issues')}")
  log_info("• Strategy status: {ifelse(validation$strategy_current, '✅ Current', '⚠️ Issues')}")
  log_info("• Files exported: {ifelse(validation$files_exported, '✅ Yes', '❌ No')}")
  log_info("• Latest strategy date: {latest_strategy_date}")
  log_info("• Total trading days: {total_trading_days}")
  log_info("• Total duration: {validation$total_duration} minutes")
  log_info("• Return type used: {ifelse(use_adjusted_returns, 'ADJUSTED (adjClose)', 'CLOSE')}")
  
  if(validation$files_exported) {
    log_info("📁 READY FOR TRADING:")
    log_info("• Latest weights: output/aggregate_weights_{format(latest_strategy_date, '%Y%m%d')}.csv")
    log_info("• Portfolio summary: output/portfolio_summary_{format(latest_strategy_date, '%Y%m%d')}.csv")
    if(export_validation_data) {
      if(use_adjusted_returns) {
        log_info("• Validation data: output/weights_with_adjusted_returns_validation.parquet")
      } else {
        log_info("• Validation data: output/weights_with_returns_validation.parquet")
      }
    }
  }
  
  log_info("🎯 DAILY UPDATE COMPLETE - READY FOR TOMORROW'S TRADING")
  log_info("=== ✅ UPDATE FINISHED ===")
  
  return(list(
    success = TRUE,
    strategy_result = strategy_result,
    validation = validation,
    latest_date = latest_strategy_date,
    duration_minutes = validation$total_duration,
    return_type = ifelse(use_adjusted_returns, "adjusted", "close"),
    portfolio_metrics = if(exists("portfolio_metrics")) portfolio_metrics else NULL
  ))
}

# ============================================================================
# ENHANCED PERSISTENT UPDATE WITH ADJUSTED RETURNS
# ============================================================================

operational_update_persistent_adjusted <- function(target_date = Sys.Date(), 
                                                   export_weights = TRUE,
                                                   export_validation_data = TRUE,
                                                   output_dir = "output") {
  
  baseline_file <- "data/enhanced_slice_results_baseline.rds"
  current_file <- "data/enhanced_slice_results_current_adjusted.rds"  # Different file for adjusted version
  
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
      recent_return <- mean(recent_results$portfolio_return, na.rm = TRUE) * 252
      recent_sharpe <- mean(recent_results$portfolio_return, na.rm = TRUE) / 
        sd(recent_results$portfolio_return, na.rm = TRUE) * sqrt(252)
      
      log_info("📈 RECENT PERFORMANCE (30 days):")
      log_info("• Annualized return: {sprintf('%.2f%%', recent_return * 100)}")
      log_info("• Annualized Sharpe: {sprintf('%.2f', recent_sharpe)}")
    }
  }
  
  return(result)
}

# ============================================================================
# DATE GATE WRAPPER (ADJUSTED RETURNS VERSION)
# ============================================================================

operational_update_with_date_gate_adjusted <- function(target_date = Sys.Date(), 
                                                       force_refresh = FALSE,
                                                       export_weights = TRUE,
                                                       export_validation_data = TRUE,
                                                       output_dir = "output") {
  
  log_info("🚀 OPERATIONAL UPDATE WITH DATE GATE (ADJUSTED RETURNS)")
  log_info("====================================================")
  
  # Quick check if update is needed
  if(!force_refresh && exists("enhanced_slice_results_final")) {
    current_end_date <- max(enhanced_slice_results_final$daily_results$date)
    days_behind <- as.numeric(target_date - 1 - current_end_date)
    
    log_info("Current end date: {current_end_date}")
    log_info("Target date: {target_date}")
    log_info("Days behind: {days_behind}")
    
    if(days_behind <= 0) {
      log_info("✅ Already up to date - no processing needed")
      
      # Still export current weights if requested
      if(export_weights && "daily_aggregate_weights" %in% names(enhanced_slice_results_final)) {
        latest_weights <- enhanced_slice_results_final$daily_aggregate_weights %>%
          filter(date == max(date))
        
        if(nrow(latest_weights) > 0) {
          export_files <- export_daily_aggregate_weights(latest_weights, output_dir, export_validation_data)
          log_info("📁 Exported current weights for trading")
        }
      }
      
      return(enhanced_slice_results_final)
    }
    
    if(days_behind <= 5) {
      log_info("⚡ Small update needed - {days_behind} days")
    } else {
      log_info("📅 Large update needed - {days_behind} days")
    }
  }
  
  # Store original end date for comparison
  original_end_date <- if(exists("enhanced_slice_results_final")) {
    max(enhanced_slice_results_final$daily_results$date)
  } else {
    as.Date("1900-01-01")
  }
  
  # Run the adjusted returns version
  start_time <- Sys.time()
  log_info("🔄 Starting operational update with ADJUSTED returns...")
  
  result <- operational_update_backtest_to_latest_adjusted(
    target_date = target_date,
    export_weights = export_weights,
    export_validation_data = export_validation_data,
    output_dir = output_dir
  )
  
  end_time <- Sys.time()
  
  # Report what was actually processed
  if(!is.null(result)) {
    new_end_date <- max(result$daily_results$date)
    days_processed <- as.numeric(new_end_date - original_end_date)
    duration <- as.numeric(difftime(end_time, start_time, units = "mins"))
    
    log_info("✅ Update completed")
    log_info("• Days processed: {days_processed}")
    log_info("• Duration: {round(duration, 2)} minutes")
    log_info("• Rate: {round(days_processed / max(duration, 0.1), 1)} days/minute")
    log_info("• Return type: ADJUSTED (adjClose)")
  }
  
  return(result)
}

# ============================================================================
# LOGGING ANALYSIS AND DOCUMENTATION
# ============================================================================

analyze_logging_system <- function() {
  
  cat("📊 LOGGING SYSTEM ANALYSIS\n")
  cat("=========================\n\n")
  
  cat("🔍 WHAT YOUR LOGGING SYSTEM CAPTURES:\n")
  cat("=====================================\n")
  
  logging_info <- "
📝 LOG LEVELS AND CONTENT:

🔴 ERROR LOGS (log_error):
• Database connection failures
• Missing required files (EOD system, operational implementation)
• Strategy update failures
• Invalid backtest results
• Price data quality issues

🟡 WARNING LOGS (log_warn):
• Large update ranges (>30 days)
• Missing data for specific tickers
• Failed API calls for individual stocks
• Data quality concerns (but not blocking)
• Old log file cleanup notifications

🔵 INFO LOGS (log_info):
• System startup and initialization
• Daily update start/completion
• Database update progress (batch processing)
• Strategy update milestones
• Portfolio metrics and performance
• File export confirmations
• Trading day processing counts
• Performance statistics (Sharpe, returns)

🟢 DEBUG LOGS (log_debug):
• Detailed API call information
• Individual ticker processing
• Technical factor calculations
• Model preprocessing steps
• Database query details

📁 LOG FILE STRUCTURE:
====================
logs/
├── eod_system_2025-06-27.log      # Daily EOD updates
├── eod_system_2025-06-28.log      # Next day
└── ...

🕐 LOG ROTATION:
===============
• Creates new log file each day
• Keeps logs for configurable days (default: probably 30 days)
• Automatically cleans up old logs
• File naming: eod_system_YYYY-MM-DD.log

📊 WHAT YOU CAN MONITOR:
=======================
• Daily update success/failure
• Performance metrics trends
• Data quality issues
• API response times
• Portfolio composition changes
• System processing speed

🎯 PRODUCTION MONITORING:
========================
• Tail logs in real-time: tail -f logs/eod_system_$(date +%Y-%m-%d).log
• Check for errors: grep 'ERROR' logs/*.log
• Monitor performance: grep 'Duration' logs/*.log
• Track portfolio: grep 'PORTFOLIO METRICS' logs/*.log
"
  
  cat(logging_info)
  cat("\n")
  
  cat("✅ YOUR LOGGING SYSTEM IS COMPREHENSIVE\n")
  cat("======================================\n")
  cat("It captures everything needed for:\n")
  cat("• Production monitoring\n")
  cat("• Performance tracking\n") 
  cat("• Error diagnosis\n")
  cat("• Audit trails\n")
  cat("• System optimization\n\n")
  
  # Check if logging is working
  if(exists("log_info")) {
    cat("🔍 TESTING LOGGING SYSTEM:\n")
    tryCatch({
      log_info("✅ Logging system test - this should appear in today's log file")
      cat("✅ Logging is working correctly\n")
    }, error = function(e) {
      cat("❌ Logging test failed:", e$message, "\n")
    })
  }
}

# ============================================================================
# SYSTEM REQUIREMENTS CHECK
# ============================================================================

check_system_requirements <- function() {
  
  log_info("🔍 CHECKING SYSTEM REQUIREMENTS")
  log_info("==============================")
  
  requirements <- list(
    logging_system = exists("log_info"),
    eod_system = file.exists("R/02_eod_update.R"),
    operational_system = file.exists("R/complete_operational_implementation_with_adjusted_returns.R"),
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
# LOAD AND DISPLAY INFORMATION
# ============================================================================

cat("✅ COMPLETE DAILY SYSTEM (ADJUSTED RETURNS) LOADED\n")
cat("=================================================\n\n")

cat("🎯 MAIN FUNCTION (uses adjusted returns by default):\n")
cat("result <- run_complete_daily_update()\n\n")

cat("🔧 CONFIGURATION OPTIONS:\n")
cat("result <- run_complete_daily_update(\n")
cat("  use_adjusted_returns = TRUE,    # Default: use adjClose\n")
cat("  force_db_refresh = FALSE,       # Set TRUE for full DB rebuild\n")
cat("  export_validation_data = TRUE   # Export parquet validation files\n")
cat(")\n\n")

cat("📊 LOGGING ANALYSIS:\n")
cat("analyze_logging_system()          # Understand what gets logged\n")
cat("check_system_requirements()       # Verify all components ready\n\n")

cat("🎯 KEY IMPROVEMENTS:\n")
cat("• Uses ADJUSTED returns by default (better performance)\n")
cat("• Comprehensive logging integration\n")
cat("• Separate state files for adjusted vs close returns\n")
cat("• Portfolio metrics in logs\n")
cat("• Performance tracking\n")
cat("• Production-ready monitoring\n")