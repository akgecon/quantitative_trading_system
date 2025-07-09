# R/utils/database.R - Database utility functions
library(duckdb)
library(DBI)
library(logger)

get_config <- function() {
  # Look for config file in config/ directory
  config_file <- "config/config.yml"
  if(file.exists(config_file)) {
    config::get(file = config_file)
  } else {
    # Fallback to default config if file not found
    list(
      database = list(
        path = "data/dev.duckdb",
        backup_enabled = TRUE
      ),
      data = list(
        universe_file = "config/universe.csv", 
        min_date = "2000-01-01",
        batch_size = 10,
        max_api_retries = 3
      ),
      logging = list(
        level = "INFO",
        max_log_days = 30,
        file_rotation = TRUE
      )
    )
  }
}

connect_db <- function() {
  cfg <- get_config()
  tryCatch({
    con <- dbConnect(duckdb::duckdb(), dbdir = cfg$database$path)
    log_info("Database connection established")
    return(con)
  }, error = function(e) {
    log_error("Failed to connect to database: {e$message}")
    stop(e)
  })
}

backup_database <- function() {
  cfg <- get_config()
  if(cfg$database$backup_enabled) {
    backup_file <- file.path("data/backups", 
                             paste0("backup_", format(Sys.Date(), "%Y%m%d"), ".duckdb"))
    file.copy(cfg$database$path, backup_file)
    log_info("Database backed up to: {backup_file}")
  }
}