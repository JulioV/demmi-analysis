source("renv/activate.R")

library("lubridate")
library("tidyr")
library("dplyr")
library("purrr")
library("stringr")
library("SingleCaseES")
library("ggplot2")

impute.mean <- function(x) replace(x, is.na(x), mean(x, na.rm = TRUE))
impute.median <- function(x) replace(x, is.na(x), median(x, na.rm = TRUE))
Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

discard_columns_with_na_percentage <- function(data, na_percentage){
    data %>% purrr::discard(~sum(is.na(.x))/length(.x)* 100 >=na_percentage)
}

discard_columns_with_mode_percentage <- function(data, mode_percentage){
    data %>% purrr::discard(~ sum(.x == Mode(.x), na.rm=TRUE)/length(.x)* 100 >=mode_percentage)
}

discard_columns_with_zero_variance <- function(df){
  df %>% select(where(~ !is.numeric(.x) || (is.numeric(.x) && min(.x, na.rm = TRUE) != max(.x, na.rm = TRUE))))
}

impute_columns_with_mean <- function(df){
    df %>% mutate(across(where(is.numeric), impute.mean))
}

scale_columns <- function(df){
    df %>% mutate(across(where(is.numeric), function(x) (x - mean(x)) / sd(x)))
}

impute_columns_with_zero <- function(df){
    df %>% mutate(across(starts_with(c("phone_accelerometer_panda_sumdurationexertionalactivityepisode", "phone_accelerometer_panda_sumdurationnonexertionalactivityepisode", "phone_applications_foreground_rapids_sumdurationall", "phone_applications_foreground_rapids_sumdurationcalling", "phone_applications_foreground_rapids_sumdurationmessaging", "phone_applications_foreground_rapids_sumdurationsocialnetworks", "phone_applications_foreground_rapids_sumdurationnone", "phone_applications_foreground_rapids_sumdurationtop1global", "phone_applications_foreground_rapids_frequencyentropyall", "phone_applications_foreground_rapids_countall", "phone_applications_foreground_rapids_frequencyentropycalling", "phone_applications_foreground_rapids_countcalling", "phone_applications_foreground_rapids_frequencyentropymessaging", "phone_applications_foreground_rapids_countmessaging", "phone_applications_foreground_rapids_frequencyentropysocialnetworks", "phone_applications_foreground_rapids_countsocialnetworks", "phone_applications_foreground_rapids_frequencyentropynone", "phone_applications_foreground_rapids_countnone", "phone_applications_foreground_rapids_frequencyentropytop1global", "phone_applications_foreground_rapids_counttop1global", "phone_calls_rapids_missed_count", "phone_calls_rapids_missed_distinctcontacts", "phone_calls_rapids_missed_countmostfrequentcontact", "phone_calls_rapids_incoming_count", "phone_calls_rapids_incoming_distinctcontacts", "phone_calls_rapids_incoming_sumduration", "phone_calls_rapids_incoming_entropyduration", "phone_calls_rapids_incoming_countmostfrequentcontact", "phone_calls_rapids_outgoing_count", "phone_calls_rapids_outgoing_distinctcontacts", "phone_calls_rapids_outgoing_sumduration", "phone_calls_rapids_outgoing_entropyduration", "phone_calls_rapids_outgoing_countmostfrequentcontact", "phone_keyboard_rapids_sessioncount", "phone_keyboard_rapids_averagesessionlength", "phone_keyboard_rapids_averageinterkeydelay", "phone_keyboard_rapids_maxtextlength", "phone_keyboard_rapids_lastmessagelength", "phone_keyboard_rapids_totalkeyboardtouches", "phone_locations_doryab_timeattop3location", "phone_locations_doryab_loglocationvariance", "phone_locations_doryab_timeattop2location", "phone_locations_doryab_totaldistance", "phone_locations_doryab_timeathome", "phone_locations_doryab_outlierstimepercent", "phone_locations_doryab_timeattop1location", "phone_locations_doryab_varspeed", "phone_locations_doryab_numberofsignificantplaces", "phone_locations_doryab_minlengthstayatclusters", "phone_locations_doryab_avglengthstayatclusters", "phone_locations_doryab_normalizedlocationentropy", "phone_locations_doryab_radiusgyration", "phone_locations_doryab_maxlengthstayatclusters", "phone_locations_doryab_numberlocationtransitions", "phone_locations_doryab_locationentropy", "phone_locations_doryab_locationvariance", "phone_messages_rapids_received_countmostfrequentcontact", "phone_messages_rapids_received_count", "phone_messages_rapids_received_distinctcontacts", "phone_messages_rapids_sent_countmostfrequentcontact", "phone_messages_rapids_sent_count", "phone_messages_rapids_sent_distinctcontacts", "phone_screen_rapids_countepisodeunlock", "phone_screen_rapids_sumdurationunlock", "phone_screen_rapids_avgdurationunlock")), function(x) replace_na(x, 0)))
}

sensed_days_per_participant <- function(valid_features, pids_categories){
    # Table 1 in the paper
    valid_features %>% 
        select(c("phone_data_yield_rapids_ratiovalidyieldedhours","pid"))%>% 
        group_by(pid) %>% 
        summarise(sensed_days = n(),
                    valid_sensed_days = sum(phone_data_yield_rapids_ratiovalidyieldedhours >= 8/24, na.rm=TRUE)) %>% 
        as.data.frame() %>% 
        full_join(pids_categories) %>% 
        arrange(category, new_pids)  %>% 
        ungroup()
}
sensed_days_per_group <- function(table_valid_days){
    # Section "Valid sensed days across all participants" in the paper
    table_valid_days %>% 
        group_by(category) %>% 
        summarise(total_sensed_days = sum(sensed_days),
                total_valid_sensed_days = sum(valid_sensed_days),
                pct = total_valid_sensed_days*100/total_sensed_days)
}

clean_features_for_phase_summaries <- function(valid_features){
    valid_features %>% 
        select(c("phone_data_yield_rapids_ratiovalidyieldedhours",
        "phone_log_demmi_episode","local_segment_start_datetime",
        "phone_log_demmi_evening",
        "phone_log_demmi_morning",
        "pid",
        )) %>% 
    # filter(phone_data_yield_rapids_ratiovalidyieldedhours >= 8/24) %>% 
    # discard_columns_with_mode_percentage(90) %>% 
    group_by(pid) %>% 
    mutate(segment_date = date(lubridate::ymd_hms(local_segment_start_datetime))) %>% 
    # distinct(segment_date, .keep_all = FALSE) %>% 
    complete(segment_date = seq(min(segment_date), max(segment_date), by="days")) %>%
    # group_by(local_segment_start_datetime) %>% 
    # # filter(if_any(everything(), ~ !is.na(.)))  %>%  
    # ungroup() %>% as.data.frame() %>% 
    mutate(phone_log_demmi_evening = replace_na(phone_log_demmi_evening, -1),
    phone_log_demmi_morning = replace_na(phone_log_demmi_morning, -1),
    phone_log_demmi_episode = replace_na(phone_log_demmi_episode, FALSE)) %>% 
    select(-phone_data_yield_rapids_ratiovalidyieldedhours) %>%  
    mutate(nday = as.numeric(lubridate::time_length(segment_date - first(segment_date), unit="days")) + 1,
            phase = if_else(phone_log_demmi_episode == TRUE, "B", "A"),
            phase_change = if_else(phase == "A" & (is.na(lag(phase))| phase != lag(phase)), 1, 0),
            phase_id = cumsum(phase_change)) %>% 
    select(-local_segment_start_datetime,  -phase_change) %>% 
    # select( -phase_change) %>% 
    mutate(phase_name = paste0(phase_id, phase)) 
}

stages_per_participant <- function(valid_features, pids_categories){
    # Table 4 in the paper
    clean_features_for_phase_summaries(valid_features) %>% 
        group_by(pid, phase_name) %>% 
        summarise(phase = first(phase),
                    n = n()) %>% 
        group_by(pid, phase) %>% 
        summarise(number_phases = n(),
                    min_length = min(n),
                    max_length = max(n),
                    avg_length= mean(n)) %>% 
        full_join(pids_categories) %>% 
        arrange(category, new_pids)  %>% 
        as.data.frame() %>% 
        filter(!is.na(new_pids)) %>% 
        # filter(new_pids %in% c("P03BE","P04BE","P05BE","P07BE","P08BE","P09BE","P10BE")) %>% 
        mutate(phase_column = paste(number_phases, "-", round(avg_length,1) , "(",min_length, ",", max_length,")")) %>% 
        pivot_wider(id_cols = new_pids, names_from=phase, values_from=phase_column)

}

stages_per_group <- function(valid_features) {
    # Second paragraph of section "Smartphone data on episode vs non episode days" in paper
    clean_features_for_phase_summaries(valid_features) %>%
        group_by(pid, phase_name) %>% 
        summarise(phase = first(phase),
                    n = n()) %>% 
        group_by(phase) %>% 
        filter(pid %in% c("P6","P7","P17","P25","P22","P23","P27")) %>% 
        summarise(number_phases = n(),
                    length_1_phases = sum(n==1),
                    min_length = min(n),
                    max_length = max(n),
                    avg_length= mean(n))
}

episodes_per_daytime <- function(episode_logs) {
    # For stats in "Part 2: Binge eating Data; Episode logs"
    episode_logs %>% 
        separate(local_time, c("hour","min","sec"),":", convert=TRUE) %>% 
        mutate(hour = as.integer(hour)) %>% 
        summarise(evening = sum(hour >= 18),
                    morning = sum(hour >= 6 & hour < 12),
                    afternoon = sum(hour >= 12 & hour < 18),
                    night = sum(hour < 6))
}

episodes_per_participant <- function(episode_logs) {
    # For table 3
    episode_logs %>% 
        group_by(pid) %>% 
        summarise(episodes = n(),
            episode_with_text = sum(str_length(notes) > 0),) %>% 
        arrange(pid)
}

parse_mood_log_files <- function(mood_logs, controls) {
    tibble(filename = mood_logs) %>% # create a data frame
        # parse all files
        mutate(file_contents = map(filename, ~ read.csv(., stringsAsFactors = F, colClasses = c(local_segment = "character",day_segment = "character", log_time = "character", trigger_time = "character", 
                                                                                                open_time = "character", show_time = "character", report_session_type = "character"))),
                pid = str_match(filename, ".*/([a-zA-Z]+?[0-9]+?)/.*")[,2]) %>%
        unnest(cols = c(file_contents)) %>%
        select(-filename) %>% 
        # parse some columns
        mutate(group = ifelse(pid %in% controls, "control", "binge_eating"),
                log_time = lubridate::ymd_hms(log_time),
                show_time = lubridate::ymd_hms(show_time),
                trigger_time = lubridate::ymd_hms(trigger_time),
                open_time = lubridate::ymd_hms(open_time)) %>% 
        # ignore first and last day of each participant
        group_by(pid) %>% 
        filter(!date(show_time) %in% c(min(date(show_time)), max(date(show_time)))) %>% 
        ungroup()
}
summarize_mood_logs <- function(mood_logs, controls) {

    # 0 very happy
    # 2 neutral
    # 4 very unhappy
    mood_notifications <- parse_mood_log_files(mood_logs, controls) %>%
        group_by(pid) %>%
        summarize(group = first(group),
                    surveyed_days = length(unique(date(show_time))),
                    expected_surveys = surveyed_days * 2,
                    triggered_surveys = sum(report_session_type %in% c("triggered_responded", "triggered_ignored")),
                    trigger_rate = round(triggered_surveys / expected_surveys,3) * 100,
                    triggered_answered_surveys = sum(report_session_type == "triggered_responded"),
                    trigger_answered_rate = round(triggered_answered_surveys / triggered_surveys, 3) *100,
                    initiated_open = sum(report_session_type %in% c("initiated_responded", "initiated_ignored")),
                    initiated_answered = sum(report_session_type == "initiated_responded"),
                    initiated_answered_rate = round(initiated_answered / initiated_open, 3) * 100,
                    initiated_corrections = sum(is_correction == TRUE, na.rm = TRUE),
                    mean_answer_delay = mean(response_time, na.rm = TRUE),
                    sd_answer_delay = sd(response_time, na.rm = TRUE),
                    n_0 = sum(mood_score == 0, na.rm = TRUE), 
                    n_1 = sum(mood_score == 1, na.rm = TRUE), 
                    n_2 = sum(mood_score == 2, na.rm = TRUE), 
                    n_3 = sum(mood_score == 3, na.rm = TRUE), 
                    n_4 = sum(mood_score == 4, na.rm = TRUE),  
                    n_na = sum(is.na(mood_score), na.rm = TRUE),
                    ignored_week1 = sum(week == 1 & report_session_type == "triggered_ignored"),
                    ignored_week2 = sum(week == 2 & report_session_type == "triggered_ignored"),
                    ignored_week3 = sum(week == 3 & report_session_type == "triggered_ignored"),
                    ignored_week4 = sum(week == 4 & report_session_type == "triggered_ignored"),
                    ignored_week5 = sum(week == 5 & report_session_type == "triggered_ignored"),
                    ignored_week6 = sum(week == 6 & report_session_type == "triggered_ignored")
                    ) %>% 
        arrange(group)
        
        mood_notifications
}

compute_mood_compliance <- function(summary_mood_notifications){
    # For last paragraph of section "Mood logs across all participants"
    summary_mood_notifications %>% 
        summarise(trigger_rate_mean = mean(trigger_rate),
                    trigger_rate_sd = sd(trigger_rate),
                    trigger_rate_min = min(trigger_rate),
                    trigger_rate_max = max(trigger_rate),
                    answered_rate_mean = mean(trigger_answered_rate),
                    answered_rate_sd = sd(trigger_answered_rate),
                    answered_rate_min = min(trigger_answered_rate),
                    answered_rate_max = max(trigger_answered_rate),
                    ignored_week1 = mean(ignored_week1),
                    ignored_week2 = mean(ignored_week2),
                    ignored_week3 = mean(ignored_week3),
                    ignored_week4 = mean(ignored_week4),
                    ignored_week5 = mean(ignored_week5),
                    ignored_week6 = mean(ignored_week6),
                    ignored = (ignored_week1+ignored_week2+ignored_week3+ignored_week4+ignored_week5+ignored_week6)/6,
                    initiated_open_sum = sum(initiated_open),
                    initiated_open_mean = mean(initiated_open),
                    initiated_open_sd = sd(initiated_open),
                    initiated_open_min = min(initiated_open),
                    initiated_open_max = max(initiated_open),
                    initiated_corrections = sum(initiated_corrections),
                    initiated_corrections_pct = initiated_corrections *100 / initiated_open_sum)
}

mood_global_answer_rate <- function(summary_mood_notifications){
    # First paragraph of section "Mood logs across all participants"
    summary_mood_notifications %>% group_by(group) %>% 
        summarise(triggered_surveys = sum(triggered_surveys),
                    triggered_answered_surveys = sum(triggered_answered_surveys),
                    global_answered_rate = triggered_answered_surveys/triggered_surveys)
}

summary_sad_mood_per_group <- function(summary_mood_notifications){
    # Second paragraph of section "Mood logs across all participants"
    summary_mood_notifications %>% 
        group_by(group) %>% 
        summarise(n_3 = sum(n_3),
                    n_4 = sum(n_4),
                    triggered_answered_surveys = sum(triggered_answered_surveys),
                    initiated_answered = sum(initiated_answered),
                    total_answered = triggered_answered_surveys + initiated_answered,
                    prct_3 = n_3 * 100 / total_answered,
                    prct_4 = n_4 *100 / total_answered)
}

summary_sad_mood_per_daytime <- function(mood_logs, controls){
    # Second paragraph of section "Mood logs across all participants"
    parse_mood_log_files(mood_logs, controls)  %>% 
        group_by(group, day_segment) %>% 
        # filter(mood_score >= 3) %>% 
        summarise(sad_scores = sum(mood_score >= 3, na.rm=TRUE),
                    total_scores = n(),
                    pct_sad_scores = sad_scores * 100 / total_scores)
}

plot_mood_scores <- function(summary_mood_notifications, plot_file){
    # Figure 3
    summary_mood_notifications %>% 
        group_by(group) %>% 
        summarise(`0 (Very Positive)` = sum(n_0),
                    `1 (Positive)` = sum(n_1),
                    `2 (Neutral)` = sum(n_2),
                    `3 (Negative)` = sum(n_3),
                    `4 (Very Negative)` = sum(n_4)) %>% 
        pivot_longer(cols=c(-group)) %>% 
        mutate(group = ifelse(group == "binge_eating", "Binge Eating", "Control")) %>% 
        ggplot(aes(x=name, y=value, fill=group)) +
        geom_bar(stat="identity", position=position_dodge()) +
        geom_text(aes(label=value), vjust=-1.3, color="black",
                    position = position_dodge(0.9), size=3.5)+
        # scale_fill_brewer(palette="Paired")+
        theme_minimal() +
        theme(legend.position="top") +
        xlab("Mood Score") + ylab("Number of Responses")

    ggsave(plot_file, dpi=500, height=6, width=7.5, units="in")
}


main <- function(){
    valid_features <- read.csv(snakemake@input[["sensor_features"]]) 
    category=c("Binge eating","Binge eating",	"Binge eating",	"Binge eating",	"Binge eating",	"Binge eating",	"Binge eating",	"Binge eating",	"Binge eating",	"Binge eating",	"Control",	"Control",	"Control",	"Control",	"Control",	"Control",	"Control",	"Control",	"Control",	"Control")
    new_pids=c("P01BE","P02BE","P03BE","P04BE","P05BE","P06BE","P07BE","P08BE","P09BE","P10BE","P11C","P12C","P13C","P14C","P15C","P16C","P17C","P18C","P19C","P20C")
    pid = c("P1","P2","P6","P7","P17","P21","P22","P23","P25","P27","P3","P4","P10","P11","P12","P13","P14","P15","P16","P18")
    pids_categories = tibble(category, new_pids, pid)

    sensed_days <- sensed_days_per_participant(valid_features, pids_categories)
    write.csv(sensed_days, snakemake@output[["valid_days_per_participant"]], row.names=FALSE)
    write.csv(sensed_days_per_group(sensed_days), snakemake@output[["valid_days_per_group"]], row.names=FALSE)

    write.csv(stages_per_participant(valid_features, pids_categories), snakemake@output[["summary_stages_per_participant"]], row.names=FALSE)
    write.csv(stages_per_group(valid_features), snakemake@output[["summary_stages_per_group"]], row.names=FALSE)


    episode_logs <- tibble(filename = snakemake@input[["episode_logs"]]) %>% # create a data frame
        mutate(file_contents = map(filename, ~ read.csv(., stringsAsFactors = F, colClasses = c(local_segment = "character", local_time = "character", 
                                                                                                reported = "character", notes = "character"))),
                pid = str_match(filename, ".*/([a-zA-Z]+?[0-9]+?)/.*")[,2]) %>%
        unnest(cols = c(file_contents)) %>%
        select(-filename)

    write.csv(episodes_per_daytime(episode_logs), snakemake@output[["summary_episodes_per_daytime"]], row.names=FALSE)
    write.csv(episodes_per_participant(episode_logs), snakemake@output[["episodes_per_participant"]], row.names=FALSE)


    summary_mood_notifications <- summarize_mood_logs(snakemake@input[["mood_logs"]], snakemake@params[["controls"]])
    write.csv(compute_mood_compliance(summary_mood_notifications), snakemake@output[["mood_compliance"]], row.names=FALSE)
    write.csv(summary_sad_mood_per_daytime(snakemake@input[["mood_logs"]], snakemake@params[["controls"]]), snakemake@output[["summary_sad_mood_per_daytime"]], row.names=FALSE)
    write.csv(mood_global_answer_rate(summary_mood_notifications), snakemake@output[["mood_global_answer_rate"]], row.names=FALSE)
    write.csv(summary_sad_mood_per_group(summary_mood_notifications), snakemake@output[["summary_sad_mood_per_group"]], row.names=FALSE)

    plot_mood_scores(summary_mood_notifications, "mood_count.png")
}

main()