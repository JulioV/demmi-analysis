source("renv/activate.R")

library("tidyr")
library("purrr")
library("dplyr")
library("stringr")
library("lubridate")
library("base")

demmi_features <- function(sensor_data_files, time_segment, provider){

    # aware_log <- read.csv(snakemake@input[["sensor_data"]]) %>% 
    # distinct(log_message, local_date_time, .keep_all = TRUE)

    aware_log <-  read.csv(sensor_data_files[["sensor_data"]], stringsAsFactors = FALSE)
    aware_log <- aware_log %>% filter_data_by_segment(time_segment) %>% 
        distinct(log_message, local_date_time, .keep_all = TRUE)
    
    mood_reports <- aware_log %>% 
        mutate(log_message = case_when(log_message == "mood|open" ~"mood|open|open", 
                                        log_message == "mood|resume" ~"mood|resume|resume",
                                        log_message == "mood|close" ~"mood|close|close",
                                        str_detect(log_message, "demmimood") ~ "mood|trigger|trigger",
                                        TRUE ~ log_message),
                local_date_time = lubridate::ymd_hms(local_date_time)) %>% 
        # parse the log messages
        filter(str_detect(log_message, "mood\\|")) %>% 
        separate(log_message, sep="\\|", into = c("type", "action", "mood_score")) %>% 
        select(local_segment, local_date_time,action, mood_score) %>% 
        # group sessions by those starting with a trigger or an open event following any event except trigger
        # when the survey was triggered, events are: trigger, [open, resume, log, close] or [open, resume, close, resume, log, close]
        #   depending on whether the survey was answered immediately or not
        # when the survey was self-initiated, the open event won't happen after a trigger event
        mutate(prev_action = lag(action, default = ""),
                report_session = cumsum(action == "trigger" | (action == "open" & prev_action != "trigger"))) %>% 
        group_by(report_session) %>%
        # extract relevant information per report session
        mutate(mood_score = if_else(action == "log", as.integer(mood_score), NA_integer_),
                trigger_time = if_else(action == "trigger", local_date_time, NA_POSIXct_),
                log_time = if_else(action == "log", local_date_time, NA_POSIXct_),
                open_time = if_else(action == "open", local_date_time, NA_POSIXct_)) %>%
        summarise(mood_score = first(na.omit(mood_score)),
                    log_time = first(na.omit(log_time)),
                    trigger_time = first(na.omit(trigger_time)),
                    open_time = last(na.omit(open_time)),
                    local_segment = first(local_segment),
                    n = n()) %>%
        # label each report session
        mutate( day_segment = case_when(lubridate::hour(log_time) >= 6 & lubridate::hour(log_time) < 12~"morning",
                                        lubridate::hour(log_time) >= 18 & lubridate::hour(log_time) < 24~"evening",),
                show_time = if_else(!is.na(trigger_time), trigger_time, open_time),
                mood_score = if_else(mood_score == -1, NA_integer_, mood_score), # This happens when the survey was opened and dismissed
                report_session_type = case_when(!is.na(mood_score) & !is.na(trigger_time)~ "triggered_responded",
                                                !is.na(mood_score) & !is.na(open_time)~ "initiated_responded",
                                                is.na(mood_score) & !is.na(trigger_time) & n == 1~ "triggered_notshown",
                                                is.na(mood_score) & !is.na(trigger_time) & n > 1~ "triggered_ignored",
                                                is.na(mood_score) & !is.na(open_time) & n > 1 ~ "initiated_ignored")) %>% 
        arrange(show_time) %>% 
        mutate(
                week =  lubridate::time_length(ymd_hms(show_time) - ymd_hms(first(show_time)), unit="days")%/% 7 + 1,
                correction_time = if_else(report_session_type == "initiated_responded", as.numeric(lubridate::time_length(show_time - lag(show_time), unit="mins")), NA_real_), # minutes
                is_correction = if_else(correction_time >= 0 & correction_time < (60), TRUE, FALSE ),
                response_time = as.numeric(lubridate::time_length(log_time - show_time, unit="mins") ))  # minutes


    episode_reports <- aware_log %>% 
        filter(str_detect(log_message, "episode\\|log")) %>%  
        separate(log_message, sep="\\|", into = c(NA, "reported", "notes")) %>% 
        mutate(reported = "episode") %>% 
        select(local_segment, local_time, reported, notes)

    mood_features <- mood_reports %>% 
        select(local_segment, mood_score, day_segment, log_time) %>% 
        filter(!is.na(mood_score) & !is.na(day_segment)) %>% 
        group_by(local_segment, day_segment) %>% 
        arrange(log_time) %>% 
        summarize(mood_score = last(mood_score)) %>% 
        ungroup() %>% 
        pivot_wider(id_cols=local_segment, names_from="day_segment", values_from=mood_score)

    episode_features <- episode_reports %>% 
        group_by(local_segment) %>% 
        summarize(episode = TRUE)

    
    write.csv(mood_reports, snakemake@output[[2]], row.names = FALSE)
    write.csv(episode_reports, snakemake@output[[3]], row.names = FALSE) 
    return(full_join(mood_features, episode_features, by="local_segment"))
}