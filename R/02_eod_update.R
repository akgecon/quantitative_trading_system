# R/02_eod_update.R
# DAILY END-OF-DAY PRICE UPDATE SYSTEM
# Modular, production-ready version for laptop deployment

# ============================================================================
# DEPENDENCIES AND SETUP
# ============================================================================

suppressPackageStartupMessages({
  library(riingo)
  library(tidyverse)
  library(lubridate)
  library(duckdb)
  library(DBI)
  library(logger)
  library(config)
})

# Source utilities
source(file.path("R", "utils", "database.R"))
source(file.path("R", "utils", "logging.R"))
source(file.path("R", "utils", "validation.R"))

# ============================================================================
# CONFIGURATION
# ============================================================================

get_eod_config <- function() {
  cfg <- get_config()
  
  # Development mode overrides
  if(Sys.getenv("DEV_MODE") == "true") {
    cfg$data$batch_size <- 5
    cfg$data$max_api_retries <- 2
    log_info("🔧 Running in development mode")
  }
  
  return(cfg)
}

load_universe <- function() {
  cfg <- get_eod_config()
  universe_path <- cfg$data$universe_file
  
  if(!file.exists(universe_path)) {
    stop("Universe file not found: ", universe_path, 
         "\nPlease copy your 'Sectional 100 Universe Names.csv' to config/universe.csv")
  }
  
  tryCatch({
    # Handle different possible column names
    df_universe <- read_csv(universe_path, show_col_types = FALSE)
    
    # Find the ticker column (could be named differently)
    possible_names <- c("Sectional 100", "ticker", "Ticker", "symbol", "Symbol")
    ticker_col <- names(df_universe)[names(df_universe) %in% possible_names][1]
    
    if(is.na(ticker_col)) {
      # If no standard name found, use first column
      ticker_col <- names(df_universe)[1]
      log_warn("Using first column '{ticker_col}' as ticker column")
    }
    
    tickers <- df_universe %>%
      select(all_of(ticker_col)) %>%
      distinct() %>%
      pull() %>%
      toupper() %>%  # Ensure uppercase
      sort()
    
    log_info("Loaded {length(tickers)} tickers from universe file")
    log_debug("Sample tickers: {paste(head(tickers, 5), collapse = ', ')}")
    
    return(tickers)
    
  }, error = function(e) {
    stop("Failed to load universe: ", e$message)
  })
}

# ============================================================================
# API FETCH FUNCTIONS
# ============================================================================

setup_riingo_token <- function() {
  token <- Sys.getenv("RIINGO_TOKEN")
  if(token == "") {
    stop("RIINGO_TOKEN not found in environment variables.\n",
         "Please add it to your .Renviron file:\n",
         "RIINGO_TOKEN=your_token_here")
  }
  
  riingo_set_token(token)
  log_debug("Riingo token configured")
}

fetch_daily_prices_robust <- function(ticker, start_date, end_date) {
  cfg <- get_eod_config()
  max_retries <- cfg$data$max_api_retries
  
  for(attempt in 1:max_retries) {
    tryCatch({
      # Progressive delay for retries
      if(attempt > 1) {
        delay <- 2^(attempt-1)  # Exponential backoff
        log_debug("Retry {attempt} for {ticker} after {delay}s delay")
        Sys.sleep(delay)
      }
      
      # Fetch data from Tiingo
      data <- riingo_prices(
        ticker, 
        start = start_date, 
        end = end_date, 
        resample_frequency = "daily"
      )
      
      # Check if data returned
      if (is.null(data) || nrow(data) == 0) {
        if(attempt == max_retries) {
          log_warn("No data available for {ticker} from {start_date} to {end_date}")
          return(tibble())
        }
        next
      }
      
      # Clean and validate data
      data_clean <- data %>%
        filter(
          !is.na(close), !is.na(volume),
          close > 0, volume >= 0,
          date >= start_date, date <= end_date
        ) %>%
        mutate(
          date = as.Date(date),
          ticker = toupper(ticker),
          updated_at = Sys.time()
        ) %>%
        arrange(date)
      
      if(nrow(data_clean) == 0) {
        log_warn("No valid data for {ticker} after filtering")
        return(tibble())
      }
      
      log_debug("✅ {ticker}: {nrow(data_clean)} records from {min(data_clean$date)} to {max(data_clean$date)}")
      return(data_clean)
      
    }, error = function(e) {
      log_debug("Attempt {attempt} failed for {ticker}: {e$message}")
      if(attempt == max_retries) {
        log_error("All {max_retries} attempts failed for {ticker}: {e$message}")
        return(tibble())
      }
    })
  }
  
  return(tibble())
}

fetch_batch_prices <- function(ticker_batch, start_date, end_date) {
  cfg <- get_eod_config()
  
  log_info("Processing batch of {length(ticker_batch)} tickers")
  
  # Setup progress tracking
  batch_results <- tibble()
  successful_tickers <- character(0)
  failed_tickers <- character(0)
  
  for(i in seq_along(ticker_batch)) {
    ticker <- ticker_batch[i]
    
    # Show progress every 5 tickers
    if(i %% 5 == 0 || i == length(ticker_batch)) {
      log_info("  Progress: {i}/{length(ticker_batch)} ({round(i/length(ticker_batch)*100, 1)}%)")
    }
    
    result <- fetch_daily_prices_robust(ticker, start_date, end_date)
    
    if(nrow(result) > 0) {
      batch_results <- bind_rows(batch_results, result)
      successful_tickers <- c(successful_tickers, ticker)
    } else {
      failed_tickers <- c(failed_tickers, ticker)
    }
    
    # Rate limiting - be nice to the API
    Sys.sleep(0.1)
  }
  
  log_info("Batch complete: {length(successful_tickers)} successful, {length(failed_tickers)} failed")
  if(length(failed_tickers) > 0) {
    log_warn("Failed tickers: {paste(head(failed_tickers, 10), collapse = ', ')}{ifelse(length(failed_tickers) > 10, '...', '')}")
  }
  
  return(batch_results)
}

# ============================================================================
# DATE RANGE MANAGEMENT
# ============================================================================

get_last_update_date <- function(con) {
  tryCatch({
    if(!dbExistsTable(con, "results_pricedata_daily")) {
      log_info("Price table doesn't exist yet - will create new table")
      return(as.Date("2000-01-01"))
    }
    
    result <- dbGetQuery(con, 
                         "SELECT MAX(date) as max_date FROM results_pricedata_daily"
    )
    
    last_date <- result$max_date
    
    if(is.na(last_date) || is.null(last_date)) {
      log_info("No data in price table - starting from beginning")
      return(as.Date("2000-01-01"))
    }
    
    last_date <- as.Date(last_date)
    log_info("Last update date in database: {last_date}")
    return(last_date)
    
  }, error = function(e) {
    log_warn("Could not determine last update date: {e$message}")
    return(as.Date("2000-01-01"))
  })
}

determine_update_range <- function(con, force_full_refresh = FALSE) {
  cfg <- get_eod_config()
  
  if(force_full_refresh) {
    start_date <- as.Date(cfg$data$min_date)
    end_date <- Sys.Date()
    log_info("🔄 Force refresh: updating from {start_date} to {end_date}")
    return(list(start_date = start_date, end_date = end_date))
  }
  
  last_date <- get_last_update_date(con)
  start_date <- last_date + 1
  end_date <- Sys.Date()
  
  if(start_date > end_date) {
    log_info("✅ Database is already up to date (last date: {last_date})")
    return(NULL)
  }
  
  # For very large gaps, warn user
  days_to_update <- as.numeric(end_date - start_date + 1)
  if(days_to_update > 30) {
    log_warn("Large update range: {days_to_update} days from {start_date} to {end_date}")
    log_warn("This may take significant time. Consider using --force-refresh for full rebuild.")
  }
  
  log_info("📅 Incremental update: {start_date} to {end_date} ({days_to_update} days)")
  return(list(start_date = start_date, end_date = end_date))
}

# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

create_database_structure <- function(con) {
  # Create main price table if it doesn't exist
  if(!dbExistsTable(con, "results_pricedata_daily")) {
    dbExecute(con, "
      CREATE TABLE results_pricedata_daily (
        ticker VARCHAR,
        date DATE,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        adjOpen DOUBLE,
        adjHigh DOUBLE,
        adjLow DOUBLE,
        adjClose DOUBLE,
        adjVolume BIGINT,
        divCash DOUBLE,
        splitFactor DOUBLE,
        updated_at TIMESTAMP
      )
    ")
    log_info("Created results_pricedata_daily table")
  }
  
  # Create indices for performance
  tryCatch({
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_pricedata_ticker_date ON results_pricedata_daily(ticker, date)")
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_pricedata_date ON results_pricedata_daily(date)")
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_pricedata_ticker ON results_pricedata_daily(ticker)")
    log_debug("Database indices created/verified")
  }, error = function(e) {
    log_warn("Could not create indices: {e$message}")
  })
}

update_price_data <- function(con, universe, date_range) {
  if(is.null(date_range)) {
    log_info("No update needed")
    return(TRUE)
  }
  
  cfg <- get_eod_config()
  start_date <- date_range$start_date
  end_date <- date_range$end_date
  
  # Create backup if table exists and backup is enabled
  if(cfg$database$backup_enabled && dbExistsTable(con, "results_pricedata_daily")) {
    backup_database()
  }
  
  # Ensure database structure exists
  create_database_structure(con)
  
  # Process tickers in batches
  ticker_batches <- split(universe, ceiling(seq_along(universe) / cfg$data$batch_size))
  total_batches <- length(ticker_batches)
  
  log_info("🚀 Processing {length(universe)} tickers in {total_batches} batches")
  log_info("📊 Batch size: {cfg$data$batch_size} | Date range: {start_date} to {end_date}")
  
  all_new_data <- tibble()
  total_records <- 0
  
  # Process each batch
  for(i in seq_along(ticker_batches)) {
    log_info("📦 Processing batch {i}/{total_batches}")
    
    batch_data <- fetch_batch_prices(ticker_batches[[i]], start_date, end_date)
    
    if(nrow(batch_data) > 0) {
      all_new_data <- bind_rows(all_new_data, batch_data)
      total_records <- total_records + nrow(batch_data)
      log_info("  Batch {i} added {nrow(batch_data)} records (total: {total_records})")
    } else {
      log_warn("  Batch {i} returned no data")
    }
    
    # Longer pause between batches to be respectful to API
    if(i < total_batches) {
      log_debug("  Pausing 2 seconds between batches...")
      Sys.sleep(2)
    }
  }
  
  # Check if we got any data
  if(nrow(all_new_data) == 0) {
    log_warn("⚠️ No new data retrieved from any batch")
    return(FALSE)
  }
  
  log_info("📈 Retrieved {nrow(all_new_data)} total records across {length(unique(all_new_data$ticker))} tickers")
  
  # Write to database
  tryCatch({
    # Remove any existing data for the same date range to avoid duplicates
    if(dbExistsTable(con, "results_pricedata_daily")) {
      dbExecute(con, sprintf(
        "DELETE FROM results_pricedata_daily WHERE date >= '%s' AND date <= '%s'",
        start_date, end_date
      ))
      log_debug("Cleaned existing data for date range")
    }
    
    # Insert new data
    dbWriteTable(con, "results_pricedata_daily", all_new_data, append = TRUE)
    log_info("✅ Successfully wrote {nrow(all_new_data)} records to database")
    
    # Verify the write
    verification <- dbGetQuery(con, sprintf(
      "SELECT COUNT(*) as count FROM results_pricedata_daily WHERE date >= '%s' AND date <= '%s'",
      start_date, end_date
    ))
    log_info("📊 Verification: {verification$count} records in database for date range")
    
    return(TRUE)
    
  }, error = function(e) {
    log_error("❌ Failed to write data to database: {e$message}")
    return(FALSE)
  })
}

# ============================================================================
# MAIN EXECUTION FUNCTION
# ============================================================================

run_daily_eod_update <- function(force_full_refresh = FALSE) {
  
  log_info("=== 🚀 STARTING DAILY EOD PRICE UPDATE ===")
  start_time <- Sys.time()
  
  tryCatch({
    # Setup
    setup_logging()
    setup_riingo_token()
    
    # Connect to database
    con <- connect_db()
    on.exit({
      if(exists("con") && !is.null(con)) {
        dbDisconnect(con)
        log_debug("Database connection closed")
      }
    }, add = TRUE)
    
    # Load universe
    universe <- load_universe()
    
    # Determine what needs to be updated
    date_range <- determine_update_range(con, force_full_refresh)
    
    # Perform the update
    success <- update_price_data(con, universe, date_range)
    
    if(success) {
      # Validate the results
      validation_result <- validate_price_data(con, date_range)
      
      # Calculate timing
      end_time <- Sys.time()
      duration <- as.numeric(difftime(end_time, start_time, units = "mins"))
      
      log_info("=== ✅ EOD UPDATE COMPLETED SUCCESSFULLY ===")
      log_info("⏱️  Duration: {round(duration, 2)} minutes")
      log_info("📊 Validation: {validation_result$summary}")
      
      # Clean up old logs
      cleanup_old_logs()
      
      return(TRUE)
      
    } else {
      log_error("❌ EOD update failed during data update phase")
      return(FALSE)
    }
    
  }, error = function(e) {
    log_error("💥 Critical error in EOD update: {e$message}")
    log_error("Stacktrace: {paste(sys.calls(), collapse = ' -> ')}")
    return(FALSE)
  })
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

check_eod_system_status <- function() {
  setup_logging()
  
  log_info("=== 🔍 EOD SYSTEM STATUS CHECK ===")
  
  tryCatch({
    con <- connect_db()
    on.exit(dbDisconnect(con), add = TRUE)
    
    if(dbExistsTable(con, "results_pricedata_daily")) {
      stats <- dbGetQuery(con, "
        SELECT 
          MIN(date) as earliest_date,
          MAX(date) as latest_date,
          COUNT(DISTINCT date) as unique_dates,
          COUNT(DISTINCT ticker) as unique_tickers,
          COUNT(*) as total_records,
          AVG(CASE WHEN close > 0 AND volume >= 0 THEN 1.0 ELSE 0.0 END) as data_quality
        FROM results_pricedata_daily
      ")
      
      log_info("📊 Database Statistics:")
      log_info("  Date range: {stats$earliest_date} to {stats$latest_date}")
      log_info("  Unique dates: {stats$unique_dates}")
      log_info("  Unique tickers: {stats$unique_tickers}")
      log_info("  Total records: {format(stats$total_records, big.mark = ',')}")
      log_info("  Data quality: {round(stats$data_quality * 100, 1)}%")
      
      # Check for recent data
      days_since_update <- as.numeric(Sys.Date() - as.Date(stats$latest_date))
      if(days_since_update <= 1) {
        log_info("✅ Data is current")
      } else if(days_since_update <= 3) {
        log_warn("⚠️ Data is {days_since_update} days old")
      } else {
        log_error("❌ Data is outdated by {days_since_update} days")
      }
      
    } else {
      log_warn("📭 No price data table found")
    }
    
    # Check universe file
    universe <- load_universe()
    log_info("🎯 Universe: {length(universe)} tickers loaded")
    
    # Check configuration
    cfg <- get_eod_config()
    log_info("⚙️ Configuration:")
    log_info("  Database: {cfg$database$path}")
    log_info("  Batch size: {cfg$data$batch_size}")
    log_info("  Max retries: {cfg$data$max_api_retries}")
    
    return(TRUE)
    
  }, error = function(e) {
    log_error("Status check failed: {e$message}")
    return(FALSE)
  })
}

# Manual update for specific date range
manual_date_range_update <- function(start_date, end_date) {
  setup_logging()
  log_info("=== 🔧 MANUAL DATE RANGE UPDATE ===")
  log_info("📅 Updating: {start_date} to {end_date}")
  
  con <- connect_db()
  on.exit(dbDisconnect(con), add = TRUE)
  
  universe <- load_universe()
  date_range <- list(start_date = as.Date(start_date), end_date = as.Date(end_date))
  
  success <- update_price_data(con, universe, date_range)
  
  if(success) {
    log_info("✅ Manual update completed successfully")
  } else {
    log_error("❌ Manual update failed")
  }
  
  return(success)
}

# Test with a small subset for development
test_eod_update <- function(test_tickers = c("AAPL", "MSFT", "GOOGL"), days_back = 5) {
  setup_logging()
  log_info("=== 🧪 TESTING EOD UPDATE ===")
  log_info("Test tickers: {paste(test_tickers, collapse = ', ')}")
  
  # Temporarily override environment for testing
  old_dev_mode <- Sys.getenv("DEV_MODE")
  Sys.setenv(DEV_MODE = "true")
  on.exit(Sys.setenv(DEV_MODE = old_dev_mode), add = TRUE)
  
  setup_riingo_token()
  
  start_date <- Sys.Date() - days_back
  end_date <- Sys.Date()
  
  log_info("📅 Date range: {start_date} to {end_date}")
  
  all_data <- tibble()
  for(ticker in test_tickers) {
    data <- fetch_daily_prices_robust(ticker, start_date, end_date)
    if(nrow(data) > 0) {
      all_data <- bind_rows(all_data, data)
      log_info("✅ {ticker}: {nrow(data)} records")
    } else {
      log_warn("❌ {ticker}: No data")
    }
  }
  
  log_info("🎯 Test complete: {nrow(all_data)} total records")
  return(all_data)
}

log_info("📦 EOD Update System loaded and ready")
log_info("💡 Usage:")
log_info("  run_daily_eod_update()                    # Normal daily update")
log_info("  run_daily_eod_update(force_full_refresh = TRUE)  # Full refresh")
log_info("  check_eod_system_status()                 # Check system health")
log_info("  test_eod_update()                        # Test with sample data")