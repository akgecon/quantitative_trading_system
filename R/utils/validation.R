# R/utils/validation.R - Data validation utilities
library(duckdb)
library(DBI)
library(tidyverse)
library(logger)

validate_price_data <- function(con, date_range = NULL) {
  
  log_info("🔍 Running data validation...")
  
  validation_result <- list(
    timestamp = Sys.time(),
    status = "unknown",
    summary = "",
    issues = list(),
    metrics = list()
  )
  
  tryCatch({
    
    if(!dbExistsTable(con, "results_pricedata_daily")) {
      validation_result$status <- "error"
      validation_result$summary <- "Price data table missing"
      validation_result$issues <- append(validation_result$issues, "results_pricedata_daily table not found")
      return(validation_result)
    }
    
    # Build query filter
    date_filter <- ""
    if(!is.null(date_range)) {
      date_filter <- sprintf("WHERE date >= '%s' AND date <= '%s'", 
                             date_range$start_date, date_range$end_date)
    }
    
    # Basic statistics
    basic_stats <- dbGetQuery(con, sprintf("
      SELECT 
        MIN(date) as earliest_date,
        MAX(date) as latest_date,
        COUNT(DISTINCT date) as unique_dates,
        COUNT(DISTINCT ticker) as unique_tickers,
        COUNT(*) as total_records
      FROM results_pricedata_daily
      %s
    ", date_filter))
    
    validation_result$metrics$basic_stats <- basic_stats
    
    # Data quality checks
    quality_stats <- dbGetQuery(con, sprintf("
      SELECT 
        SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) as null_close,
        SUM(CASE WHEN close <= 0 THEN 1 ELSE 0 END) as invalid_close,
        SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as null_volume,
        SUM(CASE WHEN volume < 0 THEN 1 ELSE 0 END) as negative_volume,
        SUM(CASE WHEN high < low THEN 1 ELSE 0 END) as high_less_than_low
      FROM results_pricedata_daily
      %s
    ", date_filter))
    
    validation_result$metrics$quality_stats <- quality_stats
    
    # Determine overall status
    critical_issues <- c()
    
    if(quality_stats$null_close > 0) {
      critical_issues <- c(critical_issues, sprintf("%d null close prices", quality_stats$null_close))
    }
    if(quality_stats$invalid_close > 0) {
      critical_issues <- c(critical_issues, sprintf("%d invalid close prices", quality_stats$invalid_close))
    }
    if(quality_stats$high_less_than_low > 0) {
      critical_issues <- c(critical_issues, sprintf("%d records with high < low", quality_stats$high_less_than_low))
    }
    
    if(length(critical_issues) > 0) {
      validation_result$status <- "error"
      validation_result$summary <- sprintf("Critical issues: %s", paste(critical_issues, collapse = ", "))
    } else {
      validation_result$status <- "healthy"
      validation_result$summary <- sprintf("Data quality good: %s records, %s tickers", 
                                           format(basic_stats$total_records, big.mark = ","),
                                           basic_stats$unique_tickers)
    }
    
    log_info("✅ Validation complete - Status: {validation_result$status}")
    log_info("📊 {validation_result$summary}")
    
    return(validation_result)
    
  }, error = function(e) {
    validation_result$status <- "error"
    validation_result$summary <- sprintf("Validation failed: %s", e$message)
    log_error("❌ Validation error: {e$message}")
    return(validation_result)
  })
}

check_data_freshness <- function(con, max_age_days = 1) {
  
  if(!dbExistsTable(con, "results_pricedata_daily")) {
    return(list(status = "error", message = "No price data table"))
  }
  
  latest_date_result <- dbGetQuery(con, "SELECT MAX(date) as latest_date FROM results_pricedata_daily")
  latest_date <- as.Date(latest_date_result$latest_date)
  
  if(is.na(latest_date)) {
    return(list(status = "error", message = "No data in table"))
  }
  
  days_old <- as.numeric(Sys.Date() - latest_date)
  
  if(days_old <= max_age_days) {
    return(list(status = "healthy", message = sprintf("Data is current (%.1f days old)", days_old)))
  } else if(days_old <= 3) {
    return(list(status = "warning", message = sprintf("Data is %.1f days old", days_old)))
  } else {
    return(list(status = "error", message = sprintf("Data is stale (%.1f days old)", days_old)))
  }
}

log_info("🔍 Data validation utilities loaded")