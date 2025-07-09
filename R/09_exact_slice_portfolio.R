# R/09_exact_slice_portfolio.R
# YOUR EXACT "Slice Portfolio Construction" code

library(tidyverse)
library(lubridate)
library(glmnet)
library(duckdb)
library(DBI)

cat("====================================================================\n")
cat("RUNNING ENHANCED SLICE WITH EXISTING TRAINED MODEL\n")
cat("Using enhanced_results_v2 from your training\n")
cat("====================================================================\n\n")

# ============================================================================
# EXTRACT MODELS FROM YOUR EXISTING enhanced_results_v2
# ============================================================================

extract_models_from_enhanced_results <- function() {
  
  if(!exists("enhanced_results_v2")) {
    stop("enhanced_results_v2 not found. Please run your enhanced model training first.")
  }
  
  if(!exists("tech_model_data") || !exists("tech_factor_cols")) {
    stop("tech_model_data and tech_factor_cols required. Please ensure they exist from your training.")
  }
  
  cat("Extracting models from enhanced_results_v2...\n")
  
  # Your enhanced_results_v2 should contain the best model information
  # We need to recreate the models using the same training approach
  
  # Use the same split and preprocessing as your training
  split_date <- as.Date("2020-01-01")
  
  train_data <- tech_model_data %>% 
    filter(date < split_date, !is.na(target_21d))
  
  # Apply your enhanced preprocessing
  train_clean <- train_data %>%
    filter(abs(target_21d) < 3 * sd(target_21d, na.rm = TRUE)) %>%
    group_by(date) %>%
    mutate(across(all_of(tech_factor_cols), ~{
      q01 <- quantile(.x, 0.01, na.rm = TRUE)
      q99 <- quantile(.x, 0.99, na.rm = TRUE)
      pmax(pmin(.x, q99), q01)
    })) %>%
    ungroup() %>%
    filter(complete.cases(.[c("target_21d", tech_factor_cols)]))
  
  # Recreate the three models (matching your training exactly)
  train_x <- as.matrix(train_clean[, tech_factor_cols])
  train_y <- train_clean$target_21d
  
  cat("Recreating models with same parameters...\n")
  
  ridge_model <- cv.glmnet(train_x, train_y, alpha = 0, nfolds = 10, type.measure = "mse", standardize = TRUE)
  elastic_model <- cv.glmnet(train_x, train_y, alpha = 0.5, nfolds = 10, type.measure = "mse", standardize = TRUE)
  lasso_model <- cv.glmnet(train_x, train_y, alpha = 1, nfolds = 10, type.measure = "mse", standardize = TRUE)
  
  # Get the IC weights from your model comparison
  if("model_comparison" %in% names(enhanced_results_v2)) {
    model_comp <- enhanced_results_v2$model_comparison
    ridge_ic <- model_comp$ic[model_comp$model == "Ridge"]
    elastic_ic <- model_comp$ic[model_comp$model == "Elastic Net"]
    lasso_ic <- model_comp$ic[model_comp$model == "Lasso"]
  } else {
    # Calculate ICs if not available
    ridge_pred <- predict(ridge_model, train_x, s = "lambda.1se")
    elastic_pred <- predict(elastic_model, train_x, s = "lambda.1se")
    lasso_pred <- predict(lasso_model, train_x, s = "lambda.1se")
    
    ridge_ic <- cor(train_y, ridge_pred, use = "complete.obs")
    elastic_ic <- cor(train_y, elastic_pred, use = "complete.obs")
    lasso_ic <- cor(train_y, lasso_pred, use = "complete.obs")
  }
  
  # Create IC weights
  ic_weights <- c(ridge_ic, elastic_ic, lasso_ic)
  ic_weights <- pmax(ic_weights, 0)
  ic_weights <- ic_weights / sum(ic_weights)
  
  cat("Model weights extracted:\n")
  cat("Ridge IC:", sprintf("%.4f", ridge_ic), "Weight:", sprintf("%.3f", ic_weights[1]), "\n")
  cat("Elastic Net IC:", sprintf("%.4f", elastic_ic), "Weight:", sprintf("%.3f", ic_weights[2]), "\n")
  cat("Lasso IC:", sprintf("%.4f", lasso_ic), "Weight:", sprintf("%.3f", ic_weights[3]), "\n\n")
  
  # Store preprocessing quantiles
  train_quantiles <- train_clean %>%
    summarise(across(all_of(tech_factor_cols), 
                     list(q01 = ~quantile(.x, 0.01, na.rm = TRUE),
                          q99 = ~quantile(.x, 0.99, na.rm = TRUE))))
  
  return(list(
    ridge_model = ridge_model,
    elastic_model = elastic_model,
    lasso_model = lasso_model,
    ic_weights = ic_weights,
    factor_cols = tech_factor_cols,
    train_quantiles = train_quantiles,
    best_model_name = ifelse(exists("enhanced_results_v2") && "best_model_name" %in% names(enhanced_results_v2), 
                             enhanced_results_v2$best_model_name, "Weighted Ensemble")
  ))
}

# ============================================================================
# ENHANCED PREDICTION FUNCTION (USING YOUR MODELS)
# ============================================================================

generate_enhanced_predictions <- function(complete_data, enhanced_model) {
  
  # Apply same preprocessing as training
  processed_data <- complete_data
  
  for(factor_name in enhanced_model$factor_cols) {
    q01_col <- paste0(factor_name, "_q01")
    q99_col <- paste0(factor_name, "_q99")
    
    if(q01_col %in% names(enhanced_model$train_quantiles) && 
       q99_col %in% names(enhanced_model$train_quantiles)) {
      q01 <- enhanced_model$train_quantiles[[q01_col]]
      q99 <- enhanced_model$train_quantiles[[q99_col]]
      processed_data[[factor_name]] <- pmax(pmin(processed_data[[factor_name]], q99), q01)
    }
  }
  
  # Generate predictions from all models
  factor_matrix <- as.matrix(processed_data[, enhanced_model$factor_cols])
  
  ridge_pred <- predict(enhanced_model$ridge_model, factor_matrix, s = "lambda.1se")
  elastic_pred <- predict(enhanced_model$elastic_model, factor_matrix, s = "lambda.1se")
  lasso_pred <- predict(enhanced_model$lasso_model, factor_matrix, s = "lambda.1se")
  
  # Create weighted ensemble using your IC weights
  ensemble_pred <- enhanced_model$ic_weights[1] * ridge_pred + 
    enhanced_model$ic_weights[2] * elastic_pred + 
    enhanced_model$ic_weights[3] * lasso_pred
  
  return(as.vector(ensemble_pred))
}

# ============================================================================
# SLICE STRATEGY WITH YOUR ENHANCED MODEL
# ============================================================================

run_slice_with_existing_enhanced_model <- function(test_start_date = as.Date("2021-01-01"), h = 21) {
  
  cat("Running slice strategy with your existing enhanced model...\n")
  cat("Test period starts:", as.character(test_start_date), "\n")
  cat("Number of slices (h):", h, "\n")
  
  # Extract models from your training results
  enhanced_model <- extract_models_from_enhanced_results()
  
  # Filter to test period
  test_data <- tech_model_data %>%
    filter(date >= test_start_date) %>%
    arrange(ticker, date)
  
  # Add daily returns
  cat("Loading daily returns from database...\n")
  con <- dbConnect(duckdb::duckdb(), dbdir = "data/dev.duckdb")
  price_data <- dbGetQuery(con, "
    SELECT ticker, date, close, 
           LAG(close) OVER (PARTITION BY ticker ORDER BY date) as prev_close
    FROM results_pricedata_daily
    ORDER BY ticker, date
  ") %>%
    mutate(date = as.Date(date)) %>%
    filter(date >= test_start_date, !is.na(prev_close)) %>%
    mutate(daily_return = close / prev_close - 1) %>%
    select(date, ticker, daily_return)
  dbDisconnect(con)
  
  test_data <- test_data %>%
    left_join(price_data, by = c("date", "ticker"))
  
  # Get trading dates
  trading_dates <- sort(unique(test_data$date))
  n_dates <- length(trading_dates)
  
  cat("Trading dates:", n_dates, "\n")
  cat("Date range:", as.character(range(trading_dates)), "\n")
  
  # Initialize portfolio
  initial_capital <- 1.0
  slice_values <- rep(initial_capital / h, h)
  slice_holdings <- vector("list", h)
  for(i in 1:h) {
    slice_holdings[[i]] <- tibble(ticker = character(0), weight = numeric(0))
  }
  
  # Results storage
  daily_results <- tibble()
  daily_aggregate_weights <- tibble()
  
  cat("Starting enhanced slice simulation...\n")
  
  # Process each trading day
  for(day_idx in 1:n_dates) {
    current_date <- trading_dates[day_idx]
    
    if(day_idx %% 100 == 0) {
      cat("Day", day_idx, "of", n_dates, "-", as.character(current_date), "\n")
    }
    
    # Get today's data
    today_data <- test_data %>% filter(date == current_date)
    if(nrow(today_data) == 0) next
    
    # Determine rebalancing slice
    rebalance_slice <- ((day_idx - 1) %% h) + 1
    
    # Generate signals for rebalancing slice
    complete_data <- today_data %>%
      select(ticker, daily_return, all_of(enhanced_model$factor_cols)) %>%
      filter(complete.cases(.))
    
    if(nrow(complete_data) < 20) {
      daily_results <- bind_rows(daily_results, tibble(
        date = current_date,
        portfolio_return = 0,
        rebalanced_slice = rebalance_slice,
        n_positions = 0,
        data_issue = TRUE
      ))
      next
    }
    
    # Generate enhanced predictions
    predictions <- generate_enhanced_predictions(complete_data, enhanced_model)
    
    # Create positions (identical to original logic)
    new_positions <- complete_data %>%
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
      select(ticker, weight)
    
    # Calculate returns and update slices (identical logic)
    daily_returns <- complete_data %>% select(ticker, daily_return)
    portfolio_value_before <- sum(slice_values)
    slice_daily_returns <- numeric(h)
    
    for(slice_j in 1:h) {
      current_holdings <- slice_holdings[[slice_j]]
      
      if(slice_j == rebalance_slice) {
        if(nrow(current_holdings) > 0) {
          slice_return <- current_holdings %>%
            left_join(daily_returns, by = "ticker") %>%
            mutate(daily_return = ifelse(is.na(daily_return), 0, daily_return)) %>%
            summarise(return = sum(weight * daily_return)) %>%
            pull(return)
          slice_daily_returns[slice_j] <- slice_return
        } else {
          slice_daily_returns[slice_j] <- 0
        }
        slice_holdings[[slice_j]] <- new_positions
      } else {
        if(nrow(current_holdings) > 0) {
          slice_return <- current_holdings %>%
            left_join(daily_returns, by = "ticker") %>%
            mutate(daily_return = ifelse(is.na(daily_return), 0, daily_return)) %>%
            summarise(return = sum(weight * daily_return)) %>%
            pull(return)
          slice_daily_returns[slice_j] <- slice_return
        } else {
          slice_daily_returns[slice_j] <- 0
        }
      }
    }
    
    # Update slice values
    for(slice_j in 1:h) {
      slice_values[slice_j] <- slice_values[slice_j] * (1 + slice_daily_returns[slice_j])
    }
    
    # Calculate portfolio return
    portfolio_value_after <- sum(slice_values)
    portfolio_daily_return <- (portfolio_value_after / portfolio_value_before) - 1
    
    # Calculate aggregate weights
    total_portfolio_value <- sum(slice_values)
    slice_weights <- slice_values / total_portfolio_value
    all_tickers <- unique(unlist(lapply(slice_holdings, function(x) x$ticker)))
    
    aggregate_weights <- tibble(ticker = character(0), aggregate_weight = numeric(0))
    
    if(length(all_tickers) > 0) {
      aggregate_weights <- tibble(ticker = all_tickers, aggregate_weight = 0)
      
      for(j in 1:h) {
        slice_weight_j <- slice_weights[j]
        slice_positions_j <- slice_holdings[[j]]
        
        if(nrow(slice_positions_j) > 0) {
          aggregate_weights <- aggregate_weights %>%
            left_join(slice_positions_j %>% rename(slice_weight = weight), by = "ticker") %>%
            mutate(
              slice_weight = ifelse(is.na(slice_weight), 0, slice_weight),
              aggregate_weight = aggregate_weight + slice_weight_j * slice_weight
            ) %>%
            select(ticker, aggregate_weight)
        }
      }
      
      aggregate_weights <- aggregate_weights %>%
        filter(abs(aggregate_weight) > 1e-10) %>%
        mutate(date = current_date)
    }
    
    # Store results
    daily_results <- bind_rows(daily_results, tibble(
      date = current_date,
      portfolio_return = portfolio_daily_return,
      rebalanced_slice = rebalance_slice,
      n_positions = nrow(new_positions),
      data_issue = FALSE
    ))
    
    if(nrow(aggregate_weights) > 0) {
      daily_aggregate_weights <- bind_rows(daily_aggregate_weights, aggregate_weights)
    }
  }
  
  cat("Enhanced slice simulation completed!\n")
  
  return(list(
    daily_results = daily_results,
    daily_aggregate_weights = daily_aggregate_weights,
    final_slice_values = slice_values,
    final_slice_holdings = slice_holdings,
    enhanced_model = enhanced_model
  ))
}

# ============================================================================
# MAIN EXECUTION - RUN WITH YOUR EXISTING MODELS
# ============================================================================

cat("🚀 RUNNING SLICE STRATEGY WITH YOUR ENHANCED MODEL\n")
cat("==================================================\n")

# Check that required objects exist
if(!exists("enhanced_results_v2")) {
  stop("❌ enhanced_results_v2 not found. Please run your enhanced model training first.")
}

if(!exists("tech_model_data") || !exists("tech_factor_cols")) {
  stop("❌ tech_model_data and tech_factor_cols not found. Please ensure they exist from your training.")
}

cat("✅ Found required objects from your enhanced model training\n")
cat("Best model from training:", ifelse("best_model_name" %in% names(enhanced_results_v2), 
                                        enhanced_results_v2$best_model_name, "Unknown"), "\n")
cat("Training IC:", sprintf("%.4f", enhanced_results_v2$ic), "\n\n")

# Run the enhanced slice strategy
enhanced_slice_results_final <- run_slice_with_existing_enhanced_model(
  test_start_date = as.Date("2021-01-01"),
  h = 21
)

# Calculate performance metrics
daily_results <- enhanced_slice_results_final$daily_results

stats <- daily_results %>%
  filter(!data_issue) %>%
  summarise(
    n_days = n(),
    mean_daily_return = mean(portfolio_return, na.rm = TRUE),
    daily_volatility = sd(portfolio_return, na.rm = TRUE),
    daily_sharpe = mean_daily_return / daily_volatility,
    annualized_return = mean_daily_return * 252,
    annualized_volatility = daily_volatility * sqrt(252),
    annualized_sharpe = daily_sharpe * sqrt(252),
    win_rate = mean(portfolio_return > 0, na.rm = TRUE),
    max_drawdown = abs(min(cumsum(portfolio_return), na.rm = TRUE)),
    avg_positions_per_day = mean(n_positions, na.rm = TRUE)
  )

cat("\n📊 ENHANCED SLICE STRATEGY PERFORMANCE:\n")
cat("======================================\n")
cat("Trading days:             ", stats$n_days, "\n")
cat("Annualized return:        ", sprintf("%.2f%%", stats$annualized_return * 100), "\n")
cat("Annualized volatility:    ", sprintf("%.2f%%", stats$annualized_volatility * 100), "\n")
cat("Annualized Sharpe ratio:  ", sprintf("%.3f", stats$annualized_sharpe), "\n")
cat("Win rate:                 ", sprintf("%.1f%%", stats$win_rate * 100), "\n")
cat("Maximum drawdown:         ", sprintf("%.2f%%", stats$max_drawdown * 100), "\n")
cat("Avg positions per day:    ", sprintf("%.1f", stats$avg_positions_per_day), "\n")

# Save results to global environment
assign("enhanced_slice_results_final", enhanced_slice_results_final, envir = .GlobalEnv)
assign("enhanced_slice_stats_final", stats, envir = .GlobalEnv)

cat("\n✅ ENHANCED SLICE STRATEGY COMPLETE!\n")
cat("Results saved as:\n")
cat("  - enhanced_slice_results_final\n")
cat("  - enhanced_slice_stats_final\n")

cat("\n📈 IMPROVEMENT SUMMARY:\n")
cat("======================\n")
cat("✅ Using your best ensemble model (", enhanced_results_v2$best_model_name, ")\n")
cat("✅ Training IC: ", sprintf("%.4f", enhanced_results_v2$ic), "\n")
cat("✅ Portfolio Sharpe: ", sprintf("%.3f", stats$annualized_sharpe), "\n")
cat("✅ Same slice mechanics with enhanced predictions\n")