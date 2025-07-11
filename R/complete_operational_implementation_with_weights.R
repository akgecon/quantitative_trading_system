# R/complete_operational_implementation_with_weights.R
# COMPLETE CORRECTED OPERATIONAL IMPLEMENTATION WITH DAILY AGGREGATE WEIGHTS
# Direct implementation of Full Technical Bridge logic for production with weight tracking

library(tidyverse)
library(lubridate)
library(glmnet)
library(duckdb)
library(DBI)
library(zoo)
library(TTR)
library(arrow)  # For parquet export

cat("====================================================================\n")
cat("COMPLETE OPERATIONAL IMPLEMENTATION WITH DAILY AGGREGATE WEIGHTS\n")
cat("Production-ready implementation with systematic weight tracking\n")
cat("====================================================================\n\n")

# ============================================================================
# AGGREGATE WEIGHTS CALCULATION FUNCTION
# ============================================================================

calculate_aggregate_weights <- function(slice_values, slice_holdings, current_date) {
  
  # Calculate slice weights (w_j = slice_value_j / total_portfolio_value)
  total_portfolio_value <- sum(slice_values)
  slice_weights <- slice_values / total_portfolio_value
  
  # Get all unique tickers across all slices
  all_tickers <- unique(unlist(lapply(slice_holdings, function(x) {
    if(nrow(x) > 0) x$ticker else character(0)
  })))
  
  if(length(all_tickers) == 0) {
    return(tibble(
      date = current_date,
      ticker = character(0),
      aggregate_weight = numeric(0),
      long_short = character(0),
      weight_magnitude = character(0)
    ))
  }
  
  # Initialize aggregate weights data frame
  aggregate_weights <- tibble(
    date = current_date,
    ticker = all_tickers,
    aggregate_weight = 0
  )
  
  # Calculate aggregate weight for each ticker: W_i = Σ(w_j × weight_ij)
  for(j in 1:21) {
    slice_weight_j <- slice_weights[j]
    slice_positions_j <- slice_holdings[[j]]
    
    if(nrow(slice_positions_j) > 0) {
      # Add this slice's contribution to aggregate weights
      aggregate_weights <- aggregate_weights %>%
        left_join(slice_positions_j %>% rename(slice_weight = weight), by = "ticker") %>%
        mutate(
          slice_weight = coalesce(slice_weight, 0),
          aggregate_weight = aggregate_weight + slice_weight_j * slice_weight
        ) %>%
        select(date, ticker, aggregate_weight)
    }
  }
  
  # Filter out tiny positions and add metadata
  final_weights <- aggregate_weights %>%
    filter(abs(aggregate_weight) > 1e-10) %>%
    mutate(
      long_short = case_when(
        aggregate_weight > 0 ~ "Long",
        aggregate_weight < 0 ~ "Short",
        TRUE ~ "Neutral"
      ),
      weight_magnitude = case_when(
        abs(aggregate_weight) >= 0.05 ~ "Large (≥5%)",
        abs(aggregate_weight) >= 0.02 ~ "Medium (2-5%)",
        abs(aggregate_weight) >= 0.01 ~ "Small (1-2%)",
        TRUE ~ "Tiny (<1%)"
      ),
      abs_weight = abs(aggregate_weight)
    ) %>%
    arrange(desc(abs_weight)) %>%
    select(-abs_weight)
  
  return(final_weights)
}

# ============================================================================
# EXPORT WEIGHTS AND VALIDATION DATA FUNCTION
# ============================================================================

export_daily_aggregate_weights <- function(daily_aggregate_weights, output_dir = "output", export_validation_data = TRUE) {
  
  if(!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  if(nrow(daily_aggregate_weights) == 0) {
    cat("No aggregate weights to export\n")
    return(NULL)
  }
  
  # Export latest weights for today's trading
  latest_date <- max(daily_aggregate_weights$date)
  latest_weights <- daily_aggregate_weights %>%
    filter(date == latest_date) %>%
    arrange(desc(abs(aggregate_weight)))
  
  latest_file <- file.path(output_dir, paste0("aggregate_weights_", 
                                              format(latest_date, "%Y%m%d"), ".csv"))
  
  write_csv(latest_weights, latest_file)
  
  # Export portfolio summary
  portfolio_summary <- latest_weights %>%
    summarise(
      date = latest_date,
      total_positions = n(),
      long_exposure = sum(pmax(aggregate_weight, 0)),
      short_exposure = sum(pmin(aggregate_weight, 0)),
      net_exposure = sum(aggregate_weight),
      gross_exposure = sum(abs(aggregate_weight)),
      largest_position = max(abs(aggregate_weight)),
      long_positions = sum(aggregate_weight > 0),
      short_positions = sum(aggregate_weight < 0),
      .groups = "drop"
    )
  
  summary_file <- file.path(output_dir, paste0("portfolio_summary_", 
                                               format(latest_date, "%Y%m%d"), ".csv"))
  
  write_csv(portfolio_summary, summary_file)
  
  # Export complete time series
  complete_file <- file.path(output_dir, "complete_aggregate_weights_timeseries.csv")
  write_csv(daily_aggregate_weights, complete_file)
  
  # NEW: Export validation data as parquet if requested
  validation_parquet_file <- NULL
  if(export_validation_data) {
    # Generate validation data on-the-fly for export
    validation_data <- generate_validation_data_for_export(daily_aggregate_weights)
    
    if(!is.null(validation_data) && nrow(validation_data) > 0) {
      validation_parquet_file <- file.path(output_dir, "weights_with_returns_validation.parquet")
      
      # Export selected columns as parquet
      validation_export <- validation_data %>%
        select(return_date, ticker, aggregate_weight, daily_return)
      
      write_parquet(validation_export, validation_parquet_file)
      
      cat("📦 Exported validation data:\n")
      cat("• Parquet file:", validation_parquet_file, "\n")
      cat("• Validation observations:", nrow(validation_export), "\n")
      cat("• Date range:", as.character(range(validation_export$return_date)), "\n")
    }
  }
  
  cat("📁 Exported aggregate weights:\n")
  cat("• Latest weights:", latest_file, "\n")
  cat("• Portfolio summary:", summary_file, "\n")
  cat("• Complete time series:", complete_file, "\n")
  cat("• Total positions on", format(latest_date, "%Y-%m-%d"), ":", nrow(latest_weights), "\n")
  cat("• Net exposure:", sprintf("%.2f%%", portfolio_summary$net_exposure * 100), "\n")
  cat("• Gross exposure:", sprintf("%.2f%%", portfolio_summary$gross_exposure * 100), "\n")
  
  return(list(
    latest_file = latest_file,
    summary_file = summary_file,
    complete_file = complete_file,
    validation_parquet_file = validation_parquet_file,
    portfolio_summary = portfolio_summary
  ))
}

# ============================================================================
# VALIDATION DATA GENERATION FOR EXPORT
# ============================================================================

generate_validation_data_for_export <- function(daily_aggregate_weights, db_path = "data/dev.duckdb") {
  
  cat("🔗 Generating validation data for export...\n")
  
  if(nrow(daily_aggregate_weights) == 0) {
    return(NULL)
  }
  
  # Connect to database
  con <- dbConnect(duckdb::duckdb(), dbdir = db_path)
  
  # Find price table
  price_table <- "results_pricedata.daily"
  
  # Test if table exists
  table_exists <- tryCatch({
    test_query <- sprintf("SELECT COUNT(*) as count FROM %s LIMIT 1", price_table)
    result <- dbGetQuery(con, test_query)
    TRUE
  }, error = function(e) {
    FALSE
  })
  
  if(!table_exists) {
    # Try alternative table names
    possible_names <- c("results_pricedata_daily", "pricedata", "daily_prices", "price_data")
    for(name in possible_names) {
      table_exists <- tryCatch({
        test_query <- sprintf("SELECT COUNT(*) as count FROM %s LIMIT 1", name)
        result <- dbGetQuery(con, test_query)
        price_table <- name
        TRUE
      }, error = function(e) {
        FALSE
      })
      if(table_exists) break
    }
  }
  
  if(!table_exists) {
    cat("❌ No valid price table found for validation data export\n")
    dbDisconnect(con)
    return(NULL)
  }
  
  # Get unique dates from weights (these are the T-1 dates)
  weight_dates <- sort(unique(daily_aggregate_weights$date))
  min_date <- min(weight_dates)
  max_date <- max(weight_dates)
  
  # Get next trading day for each weight date (T-1 -> T mapping)
  trading_dates_query <- sprintf("
    SELECT DISTINCT date 
    FROM %s 
    WHERE date >= '%s' AND date <= '%s'
      AND close IS NOT NULL
    ORDER BY date
  ", price_table, min_date, max_date + days(30))  # Extra buffer for next trading days
  
  all_trading_dates <- dbGetQuery(con, trading_dates_query)$date
  all_trading_dates <- as.Date(all_trading_dates)
  
  # Create T-1 -> T mapping
  date_mapping <- tibble()
  for(weight_date in weight_dates) {
    next_trading_day <- all_trading_dates[all_trading_dates > weight_date][1]
    if(!is.na(next_trading_day)) {
      date_mapping <- bind_rows(date_mapping, tibble(
        weight_date = as.Date(weight_date),
        return_date = as.Date(next_trading_day)
      ))
    }
  }
  
  # Get daily returns for all return dates
  return_dates <- unique(date_mapping$return_date)
  min_return_date <- min(return_dates)
  max_return_date <- max(return_dates)
  
  # Calculate daily returns
  returns_query <- sprintf("
    WITH price_data AS (
      SELECT ticker, date, close
      FROM %s 
      WHERE date >= '%s' AND date <= '%s'
        AND close IS NOT NULL AND close > 0
      ORDER BY ticker, date
    ),
    returns_calc AS (
      SELECT 
        curr.ticker,
        curr.date as return_date,
        curr.close / LAG(curr.close) OVER (PARTITION BY curr.ticker ORDER BY curr.date) - 1 as daily_return
      FROM price_data curr
    )
    SELECT ticker, return_date, daily_return
    FROM returns_calc
    WHERE daily_return IS NOT NULL
      AND return_date >= '%s' AND return_date <= '%s'
  ", price_table, min_return_date - days(5), max_return_date, min_return_date, max_return_date)
  
  daily_returns <- dbGetQuery(con, returns_query) %>%
    mutate(return_date = as.Date(return_date))
  
  dbDisconnect(con)
  
  # Join weights with date mapping and returns
  weights_with_returns <- daily_aggregate_weights %>%
    mutate(weight_date = as.Date(date)) %>%
    left_join(date_mapping, by = "weight_date") %>%
    left_join(daily_returns, by = c("ticker", "return_date")) %>%
    filter(!is.na(return_date)) %>%
    select(date, return_date, ticker, aggregate_weight, daily_return)
  
  cat("✅ Generated validation data with", nrow(weights_with_returns), "observations\n")
  
  return(weights_with_returns)
}

# ============================================================================
# COMPLETE TECHNICAL FACTORS CALCULATION (ALL 58 FACTORS)
# ============================================================================

calculate_complete_technical_factors <- function(price_data, current_date) {
  
  cat("🔧 Calculating 58 technical factors for", as.character(current_date), "\n")
  
  # Calculate comprehensive technical factors (exactly as in bridge)
  technical_factors <- price_data %>%
    group_by(ticker) %>%
    arrange(date) %>%
    mutate(
      # ==========================================
      # MOMENTUM FACTORS (10 factors)
      # ==========================================
      tech_mom_1d = lag(close, 1) / lag(close, 2) - 1,
      tech_mom_2d = lag(close, 1) / lag(close, 3) - 1,
      tech_mom_3d = lag(close, 1) / lag(close, 4) - 1,
      tech_mom_5d = lag(close, 1) / lag(close, 6) - 1,
      tech_mom_10d = lag(close, 1) / lag(close, 11) - 1,
      tech_mom_21d = lag(close, 1) / lag(close, 22) - 1,
      tech_mom_42d = lag(close, 1) / lag(close, 43) - 1,
      tech_mom_63d = lag(close, 1) / lag(close, 64) - 1,
      tech_mom_126d = lag(close, 1) / lag(close, 127) - 1,
      tech_mom_252d = lag(close, 1) / lag(close, 253) - 1,
      
      # ==========================================
      # VOLATILITY FACTORS (8 factors)
      # ==========================================
      daily_return = close/lag(close) - 1,
      tech_vol_5d = rollapply(lag(daily_return, 1), width = 5, FUN = sd, fill = NA, align = "right", na.rm = TRUE),
      tech_vol_10d = rollapply(lag(daily_return, 1), width = 10, FUN = sd, fill = NA, align = "right", na.rm = TRUE),
      tech_vol_21d = rollapply(lag(daily_return, 1), width = 21, FUN = sd, fill = NA, align = "right", na.rm = TRUE),
      tech_vol_42d = rollapply(lag(daily_return, 1), width = 42, FUN = sd, fill = NA, align = "right", na.rm = TRUE),
      tech_vol_63d = rollapply(lag(daily_return, 1), width = 63, FUN = sd, fill = NA, align = "right", na.rm = TRUE),
      tech_vol_ratio_5_21 = tech_vol_5d / tech_vol_21d,
      tech_vol_ratio_10_42 = tech_vol_10d / tech_vol_42d,
      tech_vol_ratio_21_63 = tech_vol_21d / tech_vol_63d,
      
      # ==========================================
      # VOLUME FACTORS (8 factors)
      # ==========================================
      tech_vol_mom_5d = rollapply(lag(volume, 1), width = 5, FUN = mean, fill = NA, align = "right", na.rm = TRUE) / 
        rollapply(lag(volume, 6), width = 5, FUN = mean, fill = NA, align = "right", na.rm = TRUE) - 1,
      tech_vol_mom_10d = rollapply(lag(volume, 1), width = 10, FUN = mean, fill = NA, align = "right", na.rm = TRUE) / 
        rollapply(lag(volume, 11), width = 10, FUN = mean, fill = NA, align = "right", na.rm = TRUE) - 1,
      tech_vol_mom_21d = rollapply(lag(volume, 1), width = 21, FUN = mean, fill = NA, align = "right", na.rm = TRUE) / 
        rollapply(lag(volume, 22), width = 21, FUN = mean, fill = NA, align = "right", na.rm = TRUE) - 1,
      
      # Volume-price correlations
      tech_vol_price_corr_10d = rollapply(cbind(lag(close, 1), lag(volume, 1)), 
                                          width = 10, 
                                          FUN = function(x) cor(x[,1], x[,2], use = "complete.obs"), 
                                          fill = NA, align = "right", by.column = FALSE),
      tech_vol_price_corr_21d = rollapply(cbind(lag(close, 1), lag(volume, 1)), 
                                          width = 21, 
                                          FUN = function(x) cor(x[,1], x[,2], use = "complete.obs"), 
                                          fill = NA, align = "right", by.column = FALSE),
      
      # ==========================================
      # MOVING AVERAGE FACTORS (8 factors)
      # ==========================================
      tech_price_ma_5 = lag(close, 1) / rollapply(lag(close, 2), width = 5, FUN = mean, fill = NA, align = "right", na.rm = TRUE) - 1,
      tech_price_ma_10 = lag(close, 1) / rollapply(lag(close, 2), width = 10, FUN = mean, fill = NA, align = "right", na.rm = TRUE) - 1,
      tech_price_ma_21 = lag(close, 1) / rollapply(lag(close, 2), width = 21, FUN = mean, fill = NA, align = "right", na.rm = TRUE) - 1,
      tech_price_ma_50 = lag(close, 1) / rollapply(lag(close, 2), width = 50, FUN = mean, fill = NA, align = "right", na.rm = TRUE) - 1,
      tech_price_ma_200 = lag(close, 1) / rollapply(lag(close, 2), width = 200, FUN = mean, fill = NA, align = "right", na.rm = TRUE) - 1,
      
      # Moving average crossovers
      ma_5 = rollapply(lag(close, 1), width = 5, FUN = mean, fill = NA, align = "right", na.rm = TRUE),
      ma_21 = rollapply(lag(close, 1), width = 21, FUN = mean, fill = NA, align = "right", na.rm = TRUE),
      ma_10 = rollapply(lag(close, 1), width = 10, FUN = mean, fill = NA, align = "right", na.rm = TRUE),
      ma_50 = rollapply(lag(close, 1), width = 50, FUN = mean, fill = NA, align = "right", na.rm = TRUE),
      ma_200 = rollapply(lag(close, 1), width = 200, FUN = mean, fill = NA, align = "right", na.rm = TRUE),
      
      tech_ma_cross_5_21 = as.numeric(ma_5 > ma_21),
      tech_ma_cross_10_50 = as.numeric(ma_10 > ma_50),
      tech_ma_cross_50_200 = as.numeric(ma_50 > ma_200),
      
      # ==========================================
      # TECHNICAL INDICATORS (8 factors)
      # ==========================================
      # RSI
      price_change = lag(close, 1) - lag(close, 2),
      up_moves = pmax(price_change, 0),
      down_moves = abs(pmin(price_change, 0)),
      avg_up_14 = rollapply(up_moves, width = 14, FUN = mean, fill = NA, align = "right", na.rm = TRUE),
      avg_down_14 = rollapply(down_moves, width = 14, FUN = mean, fill = NA, align = "right", na.rm = TRUE),
      rs_14 = avg_up_14 / pmax(avg_down_14, 0.001),
      tech_rsi_14 = 100 - (100 / (1 + rs_14)),
      
      avg_up_21 = rollapply(up_moves, width = 21, FUN = mean, fill = NA, align = "right", na.rm = TRUE),
      avg_down_21 = rollapply(down_moves, width = 21, FUN = mean, fill = NA, align = "right", na.rm = TRUE),
      rs_21 = avg_up_21 / pmax(avg_down_21, 0.001),
      tech_rsi_21 = 100 - (100 / (1 + rs_21)),
      
      # MACD
      ema_12 = rollapply(lag(close, 1), width = 12, FUN = mean, fill = NA, align = "right", na.rm = TRUE),
      ema_26 = rollapply(lag(close, 1), width = 26, FUN = mean, fill = NA, align = "right", na.rm = TRUE),
      tech_macd = ema_12 - ema_26,
      tech_macd_signal = rollapply(tech_macd, width = 9, FUN = mean, fill = NA, align = "right", na.rm = TRUE),
      tech_macd_histogram = tech_macd - tech_macd_signal,
      
      # ==========================================
      # RANGE AND BREAKOUT FACTORS (8 factors)
      # ==========================================
      tech_hl_range_5d = (rollapply(lag(high, 1), width = 5, FUN = max, fill = NA, align = "right", na.rm = TRUE) - 
                            rollapply(lag(low, 1), width = 5, FUN = min, fill = NA, align = "right", na.rm = TRUE)) / lag(close, 1),
      tech_hl_range_21d = (rollapply(lag(high, 1), width = 21, FUN = max, fill = NA, align = "right", na.rm = TRUE) - 
                             rollapply(lag(low, 1), width = 21, FUN = min, fill = NA, align = "right", na.rm = TRUE)) / lag(close, 1),
      
      # 52-week ratios
      tech_52w_high_ratio = lag(close, 1) / rollapply(lag(close, 2), width = 252, FUN = max, fill = NA, align = "right", na.rm = TRUE),
      tech_52w_low_ratio = lag(close, 1) / rollapply(lag(close, 2), width = 252, FUN = min, fill = NA, align = "right", na.rm = TRUE),
      
      # Breakout signals
      tech_breakout_21d = as.numeric(lag(close, 1) > rollapply(lag(close, 2), width = 21, FUN = max, fill = NA, align = "right", na.rm = TRUE)),
      tech_breakdown_21d = as.numeric(lag(close, 1) < rollapply(lag(close, 2), width = 21, FUN = min, fill = NA, align = "right", na.rm = TRUE)),
      
      # ==========================================
      # REVERSAL FACTORS (3 factors)
      # ==========================================
      tech_reversal_1d = -(close/lag(close) - 1),
      tech_reversal_2d = -rollapply(lag(close/lag(close) - 1, 1), width = 2, FUN = sum, fill = NA, align = "right", na.rm = TRUE),
      tech_reversal_5d = -rollapply(lag(close/lag(close) - 1, 1), width = 5, FUN = sum, fill = NA, align = "right", na.rm = TRUE),
      
      # ==========================================
      # INTERACTION FACTORS (4 factors)
      # ==========================================
      tech_mom_vol_21d = tech_mom_21d / tech_vol_21d,
      tech_mom_vol_63d = tech_mom_63d / tech_vol_63d,
      tech_vol_mom_strength = tech_vol_mom_21d * tech_mom_21d,
      tech_rsi_mom = (tech_rsi_14 - 50) * tech_mom_21d
    ) %>%
    ungroup()
  
  # Add cross-sectional rankings (11 factors)
  technical_factors <- technical_factors %>%
    group_by(date) %>%
    mutate(
      tech_mom_rank_21d = percent_rank(tech_mom_21d),
      tech_mom_rank_63d = percent_rank(tech_mom_63d),
      tech_mom_rank_126d = percent_rank(tech_mom_126d),
      tech_vol_rank_21d = percent_rank(-tech_vol_21d),
      tech_vol_rank_63d = percent_rank(-tech_vol_63d),
      tech_vol_rank = percent_rank(tech_vol_mom_21d),
      tech_rsi_rank = percent_rank(tech_rsi_14),
      market_return_21d = median(tech_mom_21d, na.rm = TRUE),
      tech_relative_strength = tech_mom_21d - market_return_21d,
      tech_rs_rank = percent_rank(tech_relative_strength)
    ) %>%
    ungroup()
  
  # Filter to current date and select technical factors
  factor_data <- technical_factors %>%
    filter(date == current_date) %>%
    select(ticker, starts_with("tech_"))
  
  cat("✅ Calculated factors for", nrow(factor_data), "tickers\n")
  
  return(factor_data)
}

# ============================================================================
# MODEL PREPROCESSING (EXACT BRIDGE LOGIC)
# ============================================================================

apply_enhanced_model_preprocessing <- function(factor_data, enhanced_model) {
  
  # Get all technical factor columns
  tech_factor_cols <- names(factor_data)[str_starts(names(factor_data), "tech_")]
  
  # Apply preprocessing exactly as in bridge
  processed_data <- factor_data %>%
    # Handle missing values and outliers
    mutate(across(all_of(tech_factor_cols), ~{
      x <- .
      x[is.infinite(x)] <- NA
      x
    }))
  
  # Apply training quantiles (winsorization)
  for(factor_name in tech_factor_cols) {
    if(factor_name %in% enhanced_model$factor_cols) {
      q01_col <- paste0(factor_name, "_q01")
      q99_col <- paste0(factor_name, "_q99")
      
      if(q01_col %in% names(enhanced_model$train_quantiles) && 
         q99_col %in% names(enhanced_model$train_quantiles)) {
        q01 <- enhanced_model$train_quantiles[[q01_col]]
        q99 <- enhanced_model$train_quantiles[[q99_col]]
        processed_data[[factor_name]] <- pmax(pmin(processed_data[[factor_name]], q99), q01)
      }
    }
  }
  
  # Fill remaining missing values with median
  for(col in tech_factor_cols) {
    if(col %in% names(processed_data)) {
      if(sum(!is.na(processed_data[[col]])) > 0) {
        median_val <- median(processed_data[[col]], na.rm = TRUE)
        processed_data[[col]][is.na(processed_data[[col]])] <- median_val
      } else {
        processed_data[[col]][is.na(processed_data[[col]])] <- 0
      }
    }
  }
  
  # Filter complete cases
  complete_data <- processed_data %>%
    filter(rowSums(is.na(select(., all_of(enhanced_model$factor_cols)))) < length(enhanced_model$factor_cols) * 0.3)
  
  return(complete_data)
}

# ============================================================================
# ENSEMBLE PREDICTIONS (EXACT BRIDGE LOGIC)
# ============================================================================

generate_enhanced_ensemble_predictions <- function(processed_data, enhanced_model) {
  
  # Generate predictions from all models
  factor_matrix <- as.matrix(processed_data[, enhanced_model$factor_cols])
  
  ridge_pred <- predict(enhanced_model$ridge_model, factor_matrix, s = "lambda.1se")
  elastic_pred <- predict(enhanced_model$elastic_model, factor_matrix, s = "lambda.1se")
  lasso_pred <- predict(enhanced_model$lasso_model, factor_matrix, s = "lambda.1se")
  
  # Create weighted ensemble using IC weights
  ensemble_pred <- enhanced_model$ic_weights[1] * ridge_pred + 
    enhanced_model$ic_weights[2] * elastic_pred + 
    enhanced_model$ic_weights[3] * lasso_pred
  
  return(as.vector(ensemble_pred))
}

# ============================================================================
# POSITION CREATION (EXACT BRIDGE LOGIC)
# ============================================================================

create_new_slice_positions <- function(complete_data, predictions) {
  
  # Create positions using exact bridge logic
  new_positions <- complete_data %>%
    select(ticker) %>%
    mutate(predicted_return = predictions) %>%
    mutate(decile = ntile(predicted_return, 10)) %>%
    filter(decile %in% c(1, 10)) %>%
    mutate(
      weight = case_when(
        decile == 10 ~ 0.5 / sum(decile == 10),
        decile == 1 ~ -0.5 / sum(decile == 1),
        TRUE ~ 0
      )
    ) %>%
    select(ticker, weight) %>%
    filter(abs(weight) > 1e-10)
  
  return(new_positions)
}

# ============================================================================
# SLICE RETURNS CALCULATION (EXACT BRIDGE LOGIC)
# ============================================================================

calculate_daily_slice_returns <- function(current_slice_holdings, daily_returns, rebalance_slice, new_positions) {
  
  slice_daily_returns <- numeric(21)
  
  for(slice_j in 1:21) {
    current_holdings <- current_slice_holdings[[slice_j]]
    
    if(slice_j == rebalance_slice) {
      # For rebalancing slice, use OLD positions for return calculation
      if(nrow(current_holdings) > 0) {
        slice_return <- current_holdings %>%
          left_join(daily_returns, by = "ticker") %>%
          mutate(daily_return = coalesce(daily_return, 0)) %>%
          summarise(return = sum(weight * daily_return)) %>%
          pull(return)
        slice_daily_returns[slice_j] <- slice_return
      } else {
        slice_daily_returns[slice_j] <- 0
      }
    } else {
      # For non-rebalancing slices, use existing positions
      if(nrow(current_holdings) > 0) {
        slice_return <- current_holdings %>%
          left_join(daily_returns, by = "ticker") %>%
          mutate(daily_return = coalesce(daily_return, 0)) %>%
          summarise(return = sum(weight * daily_return)) %>%
          pull(return)
        slice_daily_returns[slice_j] <- slice_return
      } else {
        slice_daily_returns[slice_j] <- 0
      }
    }
  }
  
  return(slice_daily_returns)
}

# ============================================================================
# DAILY RETURNS DATA RETRIEVAL
# ============================================================================

get_daily_stock_returns <- function(current_date, con, price_table) {
  
  # Get previous trading day
  prev_date_query <- sprintf("
    SELECT MAX(date) as prev_date 
    FROM %s 
    WHERE date < '%s'
  ", price_table, current_date)
  
  prev_date <- dbGetQuery(con, prev_date_query)$prev_date[1]
  
  if(is.na(prev_date)) {
    return(tibble(ticker = character(0), daily_return = numeric(0)))
  }
  
  # Calculate daily returns
  returns_query <- sprintf("
    SELECT 
      curr.ticker,
      curr.close / prev.close - 1 as daily_return
    FROM 
      (SELECT ticker, close FROM %s WHERE date = '%s') curr
    INNER JOIN 
      (SELECT ticker, close FROM %s WHERE date = '%s') prev
    ON curr.ticker = prev.ticker
    WHERE curr.close IS NOT NULL AND prev.close IS NOT NULL
      AND prev.close > 0
  ", price_table, current_date, price_table, prev_date)
  
  daily_returns <- dbGetQuery(con, returns_query) %>%
    select(ticker, daily_return)
  
  return(daily_returns)
}

# ============================================================================
# COMPLETE OPERATIONAL BACKTEST UPDATE WITH AGGREGATE WEIGHTS
# ============================================================================

operational_update_backtest_to_latest <- function(target_date = Sys.Date(), export_weights = TRUE, export_validation_data = TRUE, output_dir = "output") {
  
  cat("📈 OPERATIONAL BACKTEST UPDATE WITH AGGREGATE WEIGHTS\n")
  cat("====================================================\n")
  cat("Target date:", as.character(target_date), "\n")
  cat("Export weights:", export_weights, "\n")
  cat("Export validation data:", export_validation_data, "\n")
  
  # Check prerequisites
  if(!exists("enhanced_slice_results_final")) {
    cat("❌ No existing backtest results found\n")
    return(NULL)
  }
  
  # Get current backtest state
  current_end_date <- max(enhanced_slice_results_final$daily_results$date)
  extension_start_date <- current_end_date + 1
  available_end_date <- target_date - 1
  
  cat("Current backtest ends:", as.character(current_end_date), "\n")
  
  # Check if extension needed
  if(extension_start_date > available_end_date) {
    cat("✅ Backtest already up to date\n")
    # Still calculate and export current aggregate weights
    if(export_weights && "final_slice_holdings" %in% names(enhanced_slice_results_final)) {
      current_weights <- calculate_aggregate_weights(
        enhanced_slice_results_final$final_slice_values,
        enhanced_slice_results_final$final_slice_holdings,
        current_end_date
      )
      export_files <- export_daily_aggregate_weights(current_weights, output_dir, export_validation_data)
      
      # Add current weights to results
      enhanced_slice_results_final$daily_aggregate_weights <<- current_weights
      enhanced_slice_results_final$latest_export_files <<- export_files
    }
    return(enhanced_slice_results_final)
  }
  
  cat("Extension period:", as.character(extension_start_date), "to", as.character(available_end_date), "\n")
  
  # Get enhanced model
  enhanced_model <- if("enhanced_model" %in% names(enhanced_slice_results_final)) {
    enhanced_slice_results_final$enhanced_model
  } else if(exists("enhanced_results_v2")) {
    enhanced_results_v2$enhanced_model
  } else {
    cat("❌ Enhanced model not available\n")
    return(NULL)
  }
  
  if(is.null(enhanced_model$ridge_model) || is.null(enhanced_model$factor_cols)) {
    cat("❌ Enhanced model incomplete\n")
    return(NULL)
  }
  
  cat("Enhanced model loaded with", length(enhanced_model$factor_cols), "factors\n")
  
  # Initialize state for extension
  current_slice_values <- enhanced_slice_results_final$final_slice_values
  current_slice_holdings <- enhanced_slice_results_final$final_slice_holdings
  extension_daily_results <- tibble()
  
  # Load existing daily aggregate weights if available and combine properly
  if("daily_aggregate_weights" %in% names(enhanced_slice_results_final) && 
     nrow(enhanced_slice_results_final$daily_aggregate_weights) > 0) {
    existing_weights <- enhanced_slice_results_final$daily_aggregate_weights
    cat("Found existing aggregate weights from", as.character(min(existing_weights$date)), 
        "to", as.character(max(existing_weights$date)), "\n")
    daily_aggregate_weights <- existing_weights
  } else {
    daily_aggregate_weights <- tibble()
    cat("No existing aggregate weights found - starting fresh\n")
  }
  
  # Get trading dates for extension period
  con <- dbConnect(duckdb::duckdb(), dbdir = "data/dev.duckdb")
  
  # Check what tables exist
  available_tables <- dbGetQuery(con, "SHOW TABLES")
  cat("Available tables in database:\n")
  print(available_tables$name)
  
  # Try the exact table name used in bridge
  price_table <- "results_pricedata.daily"  # Note the dot, not underscore
  
  # Test if table exists
  table_exists <- tryCatch({
    test_query <- sprintf("SELECT COUNT(*) as count FROM %s LIMIT 1", price_table)
    result <- dbGetQuery(con, test_query)
    TRUE
  }, error = function(e) {
    FALSE
  })
  
  if(!table_exists) {
    # Try alternative table names
    possible_names <- c("results_pricedata_daily", "pricedata", "daily_prices", "price_data")
    for(name in possible_names) {
      table_exists <- tryCatch({
        test_query <- sprintf("SELECT COUNT(*) as count FROM %s LIMIT 1", name)
        result <- dbGetQuery(con, test_query)
        price_table <- name
        TRUE
      }, error = function(e) {
        FALSE
      })
      if(table_exists) break
    }
  }
  
  if(!table_exists) {
    cat("❌ No valid price table found. Available tables:\n")
    print(available_tables)
    dbDisconnect(con)
    return(NULL)
  }
  
  cat("Using price table:", price_table, "\n")
  
  trading_dates_query <- sprintf("
    SELECT DISTINCT date 
    FROM %s 
    WHERE date >= '%s' AND date <= '%s'
      AND close IS NOT NULL
    ORDER BY date
  ", price_table, extension_start_date, available_end_date)
  
  trading_dates <- dbGetQuery(con, trading_dates_query)$date
  trading_dates <- as.Date(trading_dates)
  
  if(length(trading_dates) == 0) {
    cat("❌ No trading dates found for extension period\n")
    dbDisconnect(con)
    return(NULL)
  }
  
  cat("Extension trading days:", length(trading_dates), "\n")
  cat("Extension date range:", as.character(range(trading_dates)), "\n")
  
  # ============================================================================
  # MAIN EXTENSION LOOP WITH AGGREGATE WEIGHTS TRACKING
  # ============================================================================
  
  cat("🚀 Starting operational portfolio extension with weight tracking...\n")
  
  for(day_idx in seq_along(trading_dates)) {
    current_date <- trading_dates[day_idx]
    
    if(day_idx %% 5 == 0 || day_idx == length(trading_dates)) {
      cat("  Day", day_idx, "/", length(trading_dates), "-", as.character(current_date), "\n")
    }
    
    # ============================================================================
    # STEP 1: GET PRICE DATA FOR FACTOR CALCULATION
    # ============================================================================
    
    lookback_date <- current_date - days(400)  # Need 252+ days for long-term factors
    
    price_data_query <- sprintf("
      SELECT ticker, date, open, high, low, close, volume
      FROM %s 
      WHERE date >= '%s' AND date <= '%s'
        AND close IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
        AND volume IS NOT NULL AND close > 1 AND volume > 1000
      ORDER BY ticker, date
    ", price_table, lookback_date, current_date)
    
    price_data <- dbGetQuery(con, price_data_query) %>%
      mutate(date = as.Date(date))
    
    if(nrow(price_data) < 1000) {
      cat("⚠️ Insufficient price data for", as.character(current_date), "- skipping\n")
      next
    }
    
    # ============================================================================
    # STEP 2: CALCULATE TECHNICAL FACTORS
    # ============================================================================
    
    factor_data <- calculate_complete_technical_factors(price_data, current_date)
    
    if(nrow(factor_data) < 20) {
      cat("⚠️ Insufficient factor data for", as.character(current_date), "- skipping\n")
      next
    }
    
    # ============================================================================
    # STEP 3: APPLY MODEL PREPROCESSING
    # ============================================================================
    
    complete_data <- apply_enhanced_model_preprocessing(factor_data, enhanced_model)
    
    if(nrow(complete_data) < 10) {
      cat("⚠️ Insufficient complete data for", as.character(current_date), "- skipping\n")
      next
    }
    
    # ============================================================================
    # STEP 4: GENERATE PREDICTIONS
    # ============================================================================
    
    predictions <- generate_enhanced_ensemble_predictions(complete_data, enhanced_model)
    
    # ============================================================================
    # STEP 5: CREATE NEW POSITIONS
    # ============================================================================
    
    new_positions <- create_new_slice_positions(complete_data, predictions)
    
    # Determine which slice to rebalance
    total_backtest_days <- nrow(enhanced_slice_results_final$daily_results)
    extension_day_number <- total_backtest_days + day_idx
    rebalance_slice <- ((extension_day_number - 1) %% 21) + 1
    
    # ============================================================================
    # STEP 6: CALCULATE DAILY RETURNS
    # ============================================================================
    
    daily_returns <- get_daily_stock_returns(current_date, con, price_table)
    
    if(nrow(daily_returns) < 50) {
      cat("⚠️ Insufficient return data for", as.character(current_date), "- skipping\n")
      next
    }
    
    # Calculate slice returns using exact bridge logic
    slice_daily_returns <- calculate_daily_slice_returns(
      current_slice_holdings, daily_returns, rebalance_slice, new_positions
    )
    
    # ============================================================================
    # STEP 7: UPDATE PORTFOLIO STATE USING EXACT SLICE LOGIC
    # ============================================================================
    
    # Calculate portfolio return BEFORE updating slice values (like slice backtest)
    portfolio_value_before <- sum(current_slice_values)
    
    # Update slice values
    current_slice_values <- current_slice_values * (1 + slice_daily_returns)
    
    # Calculate portfolio return using exact slice portfolio logic
    portfolio_value_after <- sum(current_slice_values)
    portfolio_daily_return <- (portfolio_value_after / portfolio_value_before) - 1
    
    # Update slice holdings (rebalancing slice gets new positions)
    current_slice_holdings[[rebalance_slice]] <- new_positions
    
    # ============================================================================
    # STEP 8: CALCULATE AND STORE DAILY AGGREGATE WEIGHTS (AFTER ALL UPDATES)
    # ============================================================================
    
    # Calculate aggregate weights AFTER ALL portfolio state updates
    # These represent the weights going into the next trading day
    day_aggregate_weights <- calculate_aggregate_weights(
      current_slice_values, 
      current_slice_holdings, 
      current_date
    )
    
    # Add to cumulative daily weights
    daily_aggregate_weights <- bind_rows(daily_aggregate_weights, day_aggregate_weights)
    
    # ============================================================================
    # STEP 9: STORE DAILY RESULTS
    # ============================================================================
    
    extension_daily_results <- bind_rows(extension_daily_results, tibble(
      date = current_date,
      portfolio_return = portfolio_daily_return,
      rebalanced_slice = rebalance_slice,
      n_positions = nrow(new_positions)
    ))
  }
  
  dbDisconnect(con)
  
  # ============================================================================
  # COMBINE WITH ORIGINAL BACKTEST AND EXPORT WEIGHTS
  # ============================================================================
  
  updated_backtest <- list(
    daily_results = bind_rows(
      enhanced_slice_results_final$daily_results,
      extension_daily_results
    ),
    final_slice_values = current_slice_values,
    final_slice_holdings = current_slice_holdings,
    enhanced_model = enhanced_model,
    daily_aggregate_weights = daily_aggregate_weights  # Complete time series of weights
  )
  
  # Export aggregate weights if requested
  export_files <- NULL
  if(export_weights && nrow(daily_aggregate_weights) > 0) {
    export_files <- export_daily_aggregate_weights(daily_aggregate_weights, output_dir, export_validation_data)
    updated_backtest$latest_export_files <- export_files
  }
  
  # ============================================================================
  # SUMMARY STATISTICS
  # ============================================================================
  
  extension_days <- nrow(extension_daily_results)
  final_portfolio_value <- sum(current_slice_values)
  
  if(extension_days > 0) {
    extension_return <- prod(1 + extension_daily_results$portfolio_return) - 1
    extension_volatility <- sd(extension_daily_results$portfolio_return, na.rm = TRUE)
    extension_sharpe <- if(extension_volatility > 0) mean(extension_daily_results$portfolio_return, na.rm = TRUE) / extension_volatility * sqrt(252) else 0
    
    # Portfolio composition on latest date
    latest_weights <- daily_aggregate_weights %>%
      filter(date == max(date))
    
    if(nrow(latest_weights) > 0) {
      long_exposure <- sum(pmax(latest_weights$aggregate_weight, 0))
      short_exposure <- sum(pmin(latest_weights$aggregate_weight, 0))
      net_exposure <- sum(latest_weights$aggregate_weight)
      gross_exposure <- sum(abs(latest_weights$aggregate_weight))
      
      cat("✅ Operational backtest update completed!\n")
      cat("========================================\n")
      cat("• Extension days processed:", extension_days, "\n")
      cat("• New end date:", as.character(max(updated_backtest$daily_results$date)), "\n")
      cat("• Total backtest days:", nrow(updated_backtest$daily_results), "\n")
      cat("• Final portfolio value:", sprintf("%.8f", final_portfolio_value), "\n")
      cat("• Extension return:", sprintf("%.4f%%", extension_return * 100), "\n")
      cat("• Extension Sharpe ratio:", sprintf("%.2f", extension_sharpe), "\n\n")
      
      cat("📊 LATEST PORTFOLIO COMPOSITION:\n")
      cat("• Total positions:", nrow(latest_weights), "\n")
      cat("• Long exposure:", sprintf("%.2f%%", long_exposure * 100), "\n")
      cat("• Short exposure:", sprintf("%.2f%%", short_exposure * 100), "\n")
      cat("• Net exposure:", sprintf("%.2f%%", net_exposure * 100), "\n")
      cat("• Gross exposure:", sprintf("%.2f%%", gross_exposure * 100), "\n")
      
      if(!is.null(export_files)) {
        cat("\n📁 Exported files ready for trading:\n")
        cat("• Latest weights:", basename(export_files$latest_file), "\n")
        cat("• Portfolio summary:", basename(export_files$summary_file), "\n")
        if(!is.null(export_files$validation_parquet_file)) {
          cat("• Validation data:", basename(export_files$validation_parquet_file), "\n")
        }
      }
    }
  } else {
    cat("⚠️ No extension days processed\n")
  }
  
  return(updated_backtest)
}

# ============================================================================
# USAGE INSTRUCTIONS
# ============================================================================

cat("\n✅ COMPLETE OPERATIONAL IMPLEMENTATION WITH WEIGHTS READY\n")
cat("=========================================================\n")
cat("🎯 MAIN FUNCTION (with weight and validation export):\n")
cat("updated_backtest <- operational_update_backtest_to_latest()\n\n")
cat("🎯 DISABLE VALIDATION DATA EXPORT:\n")
cat("updated_backtest <- operational_update_backtest_to_latest(export_validation_data = FALSE)\n\n")
cat("🎯 DISABLE ALL EXPORTS:\n")
cat("updated_backtest <- operational_update_backtest_to_latest(export_weights = FALSE)\n\n")
cat("🎯 CUSTOM OUTPUT DIRECTORY:\n")
cat("updated_backtest <- operational_update_backtest_to_latest(output_dir = 'my_weights')\n\n")
cat("🎯 ACCESS DAILY WEIGHTS:\n")
cat("daily_weights <- updated_backtest$daily_aggregate_weights\n")
cat("latest_weights <- daily_weights %>% filter(date == max(date))\n\n")
cat("🎯 FEATURES:\n")
cat("• Complete implementation of all 58 technical factors\n")
cat("• Exact enhanced model preprocessing and prediction\n")
cat("• FIXED: Exact slice portfolio evolution logic (portfolio value method)\n")
cat("• Daily aggregate weights calculation and export\n")
cat("• Portfolio composition analytics\n")
cat("• Ready-to-trade weight files (CSV format)\n")
cat("• Production-ready operational system\n\n")
cat("🎯 OUTPUT FILES:\n")
cat("• aggregate_weights_YYYYMMDD.csv (latest trading weights)\n")
cat("• portfolio_summary_YYYYMMDD.csv (portfolio metrics)\n")
cat("• complete_aggregate_weights_timeseries.csv (full history)\n")
cat("• weights_with_returns_validation.parquet (validation data with returns)\n")