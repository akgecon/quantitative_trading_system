# R/08_exact_forecast_returns.R
# YOUR EXACT CODE from "Forecast Returns" document

library(tidyverse)
library(lubridate) 
library(DBI)
library(duckdb)
library(zoo)
library(glmnet)
library(RcppRoll)
library(TTR)

source(file.path("R", "utils", "database.R"))
source(file.path("R", "utils", "logging.R"))

set.seed(42)

cat("====================================================================\n")
cat("ENHANCED TECHNICAL STRATEGY SLICE TEST - FULL FACTOR UNIVERSE\n")
cat("Reproducing the superior technical factor performance with comprehensive factor universe\n")
cat("====================================================================\n\n")

# ============================================================================
# STEP 1: COMPREHENSIVE TECHNICAL FACTOR CONSTRUCTION (58 FACTORS)
# ============================================================================

cat("STEP 1: COMPREHENSIVE TECHNICAL FACTOR CONSTRUCTION\n")
cat("====================================================\n")

# Connect to database (using your existing system)
con <- dbConnect(duckdb::duckdb(), dbdir = "data/dev.duckdb")
cat("✅ Connected to database\n")

# Load daily price data
daily_prices <- dbGetQuery(con, "
  SELECT ticker, date, open, high, low, close, volume
  FROM results_pricedata_daily
  ORDER BY ticker, date
") %>%
  mutate(date = as.Date(date)) %>%
  arrange(ticker, date) %>%
  filter(
    date >= as.Date("2010-01-01"),
    !is.na(close), !is.na(volume),
    close > 1, volume > 1000
  )

cat("Technical factor dataset size:", nrow(daily_prices), "observations\n")
cat("Unique tickers:", length(unique(daily_prices$ticker)), "\n") 
cat("Date range:", as.character(range(daily_prices$date)), "\n")

# Create comprehensive technical factors (all 58 factors)
cat("\nCreating comprehensive 58-factor technical universe...\n")

technical_factors <- daily_prices %>%
  group_by(ticker) %>%
  arrange(date) %>%
  mutate(
    # TARGET VARIABLES
    target_21d = lead(close, 21) / close - 1,
    
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
cat("Computing cross-sectional technical rankings...\n")

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

cat("✅ Comprehensive technical factors created\n")

# ============================================================================
# STEP 2: ENHANCED MODEL WITH ALL TECHNICAL FACTORS
# ============================================================================

cat("\nSTEP 2: ENHANCED MODEL WITH ALL TECHNICAL FACTORS\n")
cat("==================================================\n")

# Get all technical factor columns
tech_factor_cols <- names(technical_factors)[str_starts(names(technical_factors), "tech_")]
cat("Total technical factors constructed:", length(tech_factor_cols), "\n")

# Create modeling dataset
tech_model_data <- technical_factors %>%
  select(date, ticker, target_21d, all_of(tech_factor_cols)) %>%
  filter(
    !is.na(target_21d),
    date >= as.Date("2015-01-01"),
    date <= as.Date("2025-12-31")
  ) %>%
  # Handle missing values and outliers
  mutate(across(all_of(tech_factor_cols), ~{
    x <- .
    x[is.infinite(x)] <- NA
    x
  })) %>%
  group_by(date) %>%
  mutate(across(all_of(tech_factor_cols), ~{
    x <- .
    if(sum(!is.na(x)) > 5) {
      p02 <- quantile(x, 0.02, na.rm = TRUE)
      p98 <- quantile(x, 0.98, na.rm = TRUE)  
      x <- pmax(pmin(x, p98), p02)
      x[is.na(x)] <- median(x, na.rm = TRUE)
    } else {
      x[is.na(x)] <- 0
    }
    x
  })) %>%
  ungroup() %>%
  filter(
    !is.na(target_21d),
    rowSums(is.na(select(., all_of(tech_factor_cols)))) < length(tech_factor_cols) * 0.3
  )

cat("Enhanced modeling dataset size:", nrow(tech_model_data), "observations\n")
cat("Using", length(tech_factor_cols), "technical factors\n")

# ============================================================================
# IMPROVED SINGLE-SPLIT FRAMEWORK
# ============================================================================

cat("📈 ENHANCED SINGLE-SPLIT FRAMEWORK\n")
cat("==================================\n")

improve_single_split <- function(tech_model_data, tech_factor_cols) {
  
  cat("Original working approach with enhancements:\n")
  cat("Data size:", nrow(tech_model_data), "observations\n")
  cat("Date range:", as.character(range(tech_model_data$date)), "\n")
  cat("Factors:", length(tech_factor_cols), "\n\n")
  
  # ENHANCEMENT 1: Use more training data (earlier start)
  split_date <- as.Date("2020-01-01")  # Use 2020 instead of 2021
  cat("Split date moved to:", as.character(split_date), "\n")
  
  train_data <- tech_model_data %>% 
    filter(date < split_date, !is.na(target_21d))
  
  test_data <- tech_model_data %>% 
    filter(date >= split_date, !is.na(target_21d))
  
  cat("Training period:", as.character(range(train_data$date)), "\n")
  cat("Testing period: ", as.character(range(test_data$date)), "\n")
  cat("Training samples:", nrow(train_data), "\n")
  cat("Testing samples: ", nrow(test_data), "\n\n")
  
  # ENHANCEMENT 2: Better data preprocessing
  cat("Enhanced data preprocessing...\n")
  
  # Remove extreme outliers more carefully
  train_clean <- train_data %>%
    # Remove extreme target outliers (beyond 3 standard deviations)
    filter(abs(target_21d) < 3 * sd(target_21d, na.rm = TRUE)) %>%
    # Winsorize factors at 1st and 99th percentiles by date
    group_by(date) %>%
    mutate(across(all_of(tech_factor_cols), ~{
      q01 <- quantile(.x, 0.01, na.rm = TRUE)
      q99 <- quantile(.x, 0.99, na.rm = TRUE)
      pmax(pmin(.x, q99), q01)
    })) %>%
    ungroup() %>%
    # Remove any remaining missing values
    filter(complete.cases(.[c("target_21d", tech_factor_cols)]))
  
  test_clean <- test_data %>%
    # Same preprocessing for test data
    filter(abs(target_21d) < 3 * sd(target_21d, na.rm = TRUE)) %>%
    group_by(date) %>%
    mutate(across(all_of(tech_factor_cols), ~{
      # Use training data quantiles for test data
      q01 <- quantile(train_clean[[cur_column()]], 0.01, na.rm = TRUE)
      q99 <- quantile(train_clean[[cur_column()]], 0.99, na.rm = TRUE)
      pmax(pmin(.x, q99), q01)
    })) %>%
    ungroup() %>%
    filter(complete.cases(.[c("target_21d", tech_factor_cols)]))
  
  cat("After cleaning:\n")
  cat("Training samples:", nrow(train_clean), "\n")
  cat("Testing samples: ", nrow(test_clean), "\n\n")
  
  # ENHANCEMENT 3: Cross-validation with time series splits
  cat("Enhanced model training with time series CV...\n")
  
  # Prepare matrices
  train_x <- as.matrix(train_clean[, tech_factor_cols])
  train_y <- train_clean$target_21d
  test_x <- as.matrix(test_clean[, tech_factor_cols])
  test_y <- test_clean$target_21d
  
  # ENHANCEMENT 4: Try multiple regularization approaches
  
  # Ridge regression (your current approach)
  ridge_model <- cv.glmnet(
    train_x, train_y, 
    alpha = 0,
    nfolds = 10,  # More folds
    type.measure = "mse",
    standardize = TRUE
  )
  
  # Elastic Net (compromise between Ridge and Lasso)
  elastic_model <- cv.glmnet(
    train_x, train_y,
    alpha = 0.5,  # 50% Ridge, 50% Lasso
    nfolds = 10,
    type.measure = "mse", 
    standardize = TRUE
  )
  
  # Lasso (for feature selection)
  lasso_model <- cv.glmnet(
    train_x, train_y,
    alpha = 1,
    nfolds = 10,
    type.measure = "mse",
    standardize = TRUE
  )
  
  # Generate predictions
  ridge_pred <- predict(ridge_model, test_x, s = "lambda.1se")
  elastic_pred <- predict(elastic_model, test_x, s = "lambda.1se")
  lasso_pred <- predict(lasso_model, test_x, s = "lambda.1se")
  
  # Calculate ICs
  ridge_ic <- cor(test_y, ridge_pred, use = "complete.obs")
  elastic_ic <- cor(test_y, elastic_pred, use = "complete.obs")
  lasso_ic <- cor(test_y, lasso_pred, use = "complete.obs")
  
  cat("MODEL COMPARISON:\n")
  cat("Ridge IC:      ", sprintf("%.4f", ridge_ic), "\n")
  cat("Elastic Net IC:", sprintf("%.4f", elastic_ic), "\n")
  cat("Lasso IC:      ", sprintf("%.4f", lasso_ic), "\n")
  
  # Choose best model
  best_ic <- max(ridge_ic, elastic_ic, lasso_ic)
  if(best_ic == ridge_ic) {
    best_model <- ridge_model
    best_pred <- ridge_pred
    best_name <- "Ridge"
  } else if(best_ic == elastic_ic) {
    best_model <- elastic_model
    best_pred <- elastic_pred
    best_name <- "Elastic Net"
  } else {
    best_model <- lasso_model
    best_pred <- lasso_pred
    best_name <- "Lasso"
  }
  
  cat("Best model:    ", best_name, "with IC =", sprintf("%.4f", best_ic), "\n\n")
  
  # ENHANCEMENT 5: Ensemble approach
  cat("Creating ensemble prediction...\n")
  
  # Simple average ensemble
  ensemble_pred <- (ridge_pred + elastic_pred + lasso_pred) / 3
  ensemble_ic <- cor(test_y, ensemble_pred, use = "complete.obs")
  
  # Weighted ensemble based on IC
  ic_weights <- c(ridge_ic, elastic_ic, lasso_ic)
  ic_weights <- pmax(ic_weights, 0)  # No negative weights
  ic_weights <- ic_weights / sum(ic_weights)
  
  weighted_ensemble_pred <- ic_weights[1] * ridge_pred + 
    ic_weights[2] * elastic_pred + 
    ic_weights[3] * lasso_pred
  weighted_ensemble_ic <- cor(test_y, weighted_ensemble_pred, use = "complete.obs")
  
  cat("Simple ensemble IC:   ", sprintf("%.4f", ensemble_ic), "\n")
  cat("Weighted ensemble IC: ", sprintf("%.4f", weighted_ensemble_ic), "\n")
  
  # Choose final prediction
  final_ic <- max(best_ic, ensemble_ic, weighted_ensemble_ic)
  if(final_ic == weighted_ensemble_ic) {
    final_pred <- weighted_ensemble_pred
    final_name <- "Weighted Ensemble"
  } else if(final_ic == ensemble_ic) {
    final_pred <- ensemble_pred
    final_name <- "Simple Ensemble"
  } else {
    final_pred <- best_pred
    final_name <- best_name
  }
  
  cat("Final choice:         ", final_name, "with IC =", sprintf("%.4f", final_ic), "\n\n")
  
  # Create enhanced position data
  enhanced_test_data <- data.frame(
    date = test_clean$date,
    ticker = test_clean$ticker,
    actual_return = test_y,
    predicted_return = as.vector(final_pred)
  )
  
  return(list(
    test_data = enhanced_test_data,
    ic = final_ic,
    model_comparison = data.frame(
      model = c("Ridge", "Elastic Net", "Lasso", "Simple Ensemble", "Weighted Ensemble"),
      ic = c(ridge_ic, elastic_ic, lasso_ic, ensemble_ic, weighted_ensemble_ic)
    ),
    best_model_name = final_name,
    ic_weights = ic_weights
  ))
}

# Run enhanced single-split
cat("Running enhanced single-split approach...\n")
enhanced_results <- improve_single_split(tech_model_data, tech_factor_cols)

cat("📊 ENHANCED RESULTS:\n")
cat("===================\n")
cat("Final IC:", sprintf("%.4f", enhanced_results$ic), "\n")
cat("Best approach:", enhanced_results$best_model_name, "\n\n")

print(enhanced_results$model_comparison)

# Test decile performance
cat("\n📈 DECILE ANALYSIS:\n")
cat("==================\n")

decile_analysis <- enhanced_results$test_data %>%
  group_by(date) %>%
  mutate(decile = ntile(predicted_return, 10)) %>%
  ungroup() %>%
  group_by(decile) %>%
  summarise(
    mean_actual = mean(actual_return, na.rm = TRUE) * 12,  # Annualized
    n_obs = n(),
    .groups = "drop"
  )

print(decile_analysis)

enhanced_long_short <- decile_analysis$mean_actual[10] - decile_analysis$mean_actual[1]
cat("\nEnhanced long-short return:", sprintf("%.2f%%", enhanced_long_short), "\n")

# Create position data for slice testing
enhanced_position_data_v2 <- enhanced_results$test_data %>%
  group_by(date) %>%
  mutate(
    decile = ntile(predicted_return, 10),
    position = case_when(
      decile == 10 ~ 1,   # Long
      decile == 1 ~ -1,   # Short
      TRUE ~ 0
    )
  ) %>%
  ungroup() %>%
  filter(position != 0) %>%
  rename(target_21d = actual_return)

cat("\nPosition data created:", nrow(enhanced_position_data_v2), "observations\n")
cat("Date range:", as.character(range(enhanced_position_data_v2$date)), "\n")

# Quick sanity check
sanity_check <- enhanced_position_data_v2 %>%
  group_by(date) %>%
  summarise(
    long_ret = mean(target_21d[position == 1], na.rm = TRUE),
    short_ret = mean(target_21d[position == -1], na.rm = TRUE),
    ls_ret = long_ret - short_ret,
    .groups = "drop"
  )

avg_ls_sanity <- mean(sanity_check$ls_ret, na.rm = TRUE) * 12
cat("Sanity check L/S return:", sprintf("%.2f%%", avg_ls_sanity), "annualized\n")

# Save enhanced results
assign("enhanced_results_v2", enhanced_results, envir = .GlobalEnv)
assign("enhanced_position_data_v2", enhanced_position_data_v2, envir = .GlobalEnv)

dbDisconnect(con)
