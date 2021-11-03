rule paper_data:
    input:
        sensor_features = "data/processed/features/all_participants/all_sensor_features.csv",
        mood_logs = expand("data/interim/{pid}/phone_log_features/mood_logs_demmi.csv", pid=config["PIDS_CONTROLS"] + config["PIDS_BE"]),
        episode_logs = expand("data/interim/{pid}/phone_log_features/episode_logs_demmi.csv", pid=config["PIDS_BE"])
    params:
        controls = config["PIDS_CONTROLS"]
    output:
        valid_days_per_participant = "data/processed/paper/valid_days_per_participant.csv",
        valid_days_per_group = "data/processed/paper/valid_days_per_group.csv",
        summary_stages_per_participant = "data/processed/paper/summary_stages_per_participant.csv",
        summary_stages_per_group = "data/processed/paper/summary_stages_per_group.csv",
        summary_sad_mood_per_daytime = "data/processed/paper/summary_sad_mood_per_daytime.csv",
        mood_global_answer_rate = "data/processed/paper/mood_global_answer_rate.csv",
        summary_sad_mood_per_group = "data/processed/paper/summary_sad_mood_per_group.csv",
        summary_episodes_per_daytime = "data/processed/paper/summary_episodes_per_daytime.csv",
        episodes_per_participant = "data/processed/paper/episodes_per_participant.csv",
        mood_compliance = "data/processed/paper/mood_compliance.csv",
    script:
        "../src/models/data_summary.R"
