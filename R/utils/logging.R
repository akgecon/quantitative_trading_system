# R/utils/logging.R - Logging setup
library(logger)

setup_logging <- function() {
  cfg <- get_config()
  
  # Create logs directory
  if(!dir.exists("logs")) dir.create("logs")
  
  # Set log level
  log_threshold(cfg$logging$level)
  
  # Set up file appender with date
  log_file <- file.path("logs", paste0("eod_system_", Sys.Date(), ".log"))
  log_appender(appender_tee(file = log_file))
  
  # Use colored layout
  log_layout(layout_glue_colors)
  
  log_info("Logging initialized - Level: {cfg$logging$level}")
}

cleanup_old_logs <- function() {
  cfg <- get_config()
  if(cfg$logging$file_rotation) {
    cutoff_date <- Sys.Date() - cfg$logging$max_log_days
    old_logs <- list.files("logs", pattern = "*.log", full.names = TRUE)
    
    for(log_file in old_logs) {
      file_date <- as.Date(gsub(".*_(\\d{4}-\\d{2}-\\d{2})\\.log", "\\1", basename(log_file)))
      if(!is.na(file_date) && file_date < cutoff_date) {
        file.remove(log_file)
        log_info("Removed old log file: {log_file}")
      }
    }
  }
}