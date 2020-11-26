rule restore_sql_file:
    input:
        sql_file = "data/external/rapids_example.sql",
        db_credentials = ".env"
    params:
        group = config["DATABASE_GROUP"]
    output:
        touch("data/interim/restore_sql_file.done")
    script:
        "../src/data/restore_sql_file.py"

rule create_example_participant_files:
    output:
        expand("data/external/{pid}", pid = ["example01", "example02"])
    shell:
        "echo 'a748ee1a-1d0b-4ae9-9074-279a2b6ba524\nandroid\ntest01\n2020/04/23,2020/05/04\n' >> ./data/external/example01 && echo '13dbc8a3-dae3-4834-823a-4bc96a7d459d\nios\ntest02\n2020/04/23,2020/05/04\n' >> ./data/external/example02"

rule create_participants_files:
    input:
        participants_file = [] if config["CREATE_PARTICIPANT_FILES"]["SOURCE"]["TYPE"] == "AWARE_DEVICE_TABLE" else config["CREATE_PARTICIPANT_FILES"]["SOURCE"]["CSV_FILE_PATH"] 
    params:
        config = config["CREATE_PARTICIPANT_FILES"]
    script:
        "../src/data/create_participants_files.R"

rule download_phone_data:
    input:
        "data/external/participant_files/{pid}.yaml"
    params:
        source = config["PHONE_DATA_CONFIGURATION"]["SOURCE"],
        sensor = "phone_" + "{sensor}",
        table = lambda wildcards: config["PHONE_" + str(wildcards.sensor).upper()]["TABLE"],
        timezone = config["PHONE_DATA_CONFIGURATION"]["TIMEZONE"]["VALUE"],
        aware_multiplatform_tables = config["PHONE_ACTIVITY_RECOGNITION"]["TABLE"]["ANDROID"] + "," + config["PHONE_ACTIVITY_RECOGNITION"]["TABLE"]["IOS"] + "," + config["PHONE_CONVERSATION"]["TABLE"]["ANDROID"] + "," + config["PHONE_CONVERSATION"]["TABLE"]["IOS"],
    output:
        "data/raw/{pid}/phone_{sensor}_raw.csv"
    script:
        "../src/data/download_phone_data.R"

rule download_fitbit_data:
    input:
        participant_file = "data/external/participant_files/{pid}.yaml",
        input_file = [] if config["FITBIT_DATA_CONFIGURATION"]["SOURCE"]["TYPE"] == "DATABASE" else lambda wildcards: config["FITBIT_" + str(wildcards.sensor).upper()]["TABLE"]
    params:
        data_configuration = config["FITBIT_DATA_CONFIGURATION"],
        sensor = "fitbit_" + "{sensor}",
        table = lambda wildcards: config["FITBIT_" + str(wildcards.sensor).upper()]["TABLE"],
    output:
        "data/raw/{pid}/fitbit_{sensor}_raw.csv"
    script:
        "../src/data/download_fitbit_data.R"

rule compute_day_segments:
    input: 
        config["DAY_SEGMENTS"]["FILE"],
        "data/external/participant_files/{pid}.yaml"
    params:
        day_segments_type = config["DAY_SEGMENTS"]["TYPE"],
        pid = "{pid}"
    output:
        segments_file = "data/interim/day_segments/{pid}_day_segments.csv",
        segments_labels_file = "data/interim/day_segments/{pid}_day_segments_labels.csv",
    script:
        "../src/data/compute_day_segments.py"

rule phone_readable_datetime:
    input:
        sensor_input = "data/raw/{pid}/phone_{sensor}_raw.csv",
        day_segments = "data/interim/day_segments/{pid}_day_segments.csv"
    params:
        timezones = config["PHONE_DATA_CONFIGURATION"]["TIMEZONE"]["TYPE"],
        fixed_timezone = config["PHONE_DATA_CONFIGURATION"]["TIMEZONE"]["VALUE"],
        day_segments_type = config["DAY_SEGMENTS"]["TYPE"],
        include_past_periodic_segments = config["DAY_SEGMENTS"]["INCLUDE_PAST_PERIODIC_SEGMENTS"]
    output:
        "data/raw/{pid}/phone_{sensor}_with_datetime.csv"
    script:
        "../src/data/readable_datetime.R"

rule phone_yielded_timestamps:
    input:
        all_sensors = expand("data/raw/{{pid}}/{sensor}_raw.csv", sensor = map(str.lower, config["PHONE_DATA_YIELD"]["SENSORS"]))
    params:
        sensors = config["PHONE_DATA_YIELD"]["SENSORS"] # not used but needed so the rule is triggered if this array changes
    output:
        "data/interim/{pid}/phone_yielded_timestamps.csv"
    script:
        "../src/data/phone_yielded_timestamps.R"

rule phone_yielded_timestamps_with_datetime:
    input:
        sensor_input = "data/interim/{pid}/phone_yielded_timestamps.csv",
        day_segments = "data/interim/day_segments/{pid}_day_segments.csv"
    params:
        timezones = config["PHONE_DATA_CONFIGURATION"]["TIMEZONE"]["TYPE"],
        fixed_timezone = config["PHONE_DATA_CONFIGURATION"]["TIMEZONE"]["VALUE"],
        day_segments_type = config["DAY_SEGMENTS"]["TYPE"],
        include_past_periodic_segments = config["DAY_SEGMENTS"]["INCLUDE_PAST_PERIODIC_SEGMENTS"]
    output:
        "data/interim/{pid}/phone_yielded_timestamps_with_datetime.csv"
    script:
        "../src/data/readable_datetime.R"

rule unify_ios_android:
    input:
        sensor_data = "data/raw/{pid}/{sensor}_with_datetime.csv",
        participant_info = "data/external/participant_files/{pid}.yaml"
    params:
        sensor = "{sensor}",
    output:
        "data/raw/{pid}/{sensor}_with_datetime_unified.csv"
    script:
        "../src/data/unify_ios_android.R"

rule process_phone_locations_types:
    input:
        locations = "data/raw/{pid}/phone_locations_raw.csv",
        phone_sensed_timestamps = "data/interim/{pid}/phone_yielded_timestamps.csv",
    params:
        consecutive_threshold = config["PHONE_LOCATIONS"]["FUSED_RESAMPLED_CONSECUTIVE_THRESHOLD"],
        time_since_valid_location = config["PHONE_LOCATIONS"]["FUSED_RESAMPLED_TIME_SINCE_VALID_LOCATION"],
        locations_to_use = config["PHONE_LOCATIONS"]["LOCATIONS_TO_USE"]
    output:
        "data/interim/{pid}/phone_locations_processed.csv"
    script:
        "../src/data/process_location_types.R"

rule phone_locations_processed_with_datetime:
    input:
        sensor_input = "data/interim/{pid}/phone_locations_processed.csv",
        day_segments = "data/interim/day_segments/{pid}_day_segments.csv"
    params:
        timezones = config["PHONE_DATA_CONFIGURATION"]["TIMEZONE"]["TYPE"],
        fixed_timezone = config["PHONE_DATA_CONFIGURATION"]["TIMEZONE"]["VALUE"],
        day_segments_type = config["DAY_SEGMENTS"]["TYPE"],
        include_past_periodic_segments = config["DAY_SEGMENTS"]["INCLUDE_PAST_PERIODIC_SEGMENTS"]
    output:
        "data/interim/{pid}/phone_locations_processed_with_datetime.csv"
    script:
        "../src/data/readable_datetime.R"

rule resample_episodes:
    input:
        "data/interim/{pid}/{sensor}_episodes.csv"
    output:
        "data/interim/{pid}/{sensor}_episodes_resampled.csv"
    script:
        "../src/features/utils/resample_episodes.R"

rule resample_episodes_with_datetime:
    input:
        sensor_input = "data/interim/{pid}/{sensor}_episodes_resampled.csv",
        day_segments = "data/interim/day_segments/{pid}_day_segments.csv"
    params:
        timezones = config["PHONE_DATA_CONFIGURATION"]["TIMEZONE"]["TYPE"],
        fixed_timezone = config["PHONE_DATA_CONFIGURATION"]["TIMEZONE"]["VALUE"],
        day_segments_type = config["DAY_SEGMENTS"]["TYPE"],
        include_past_periodic_segments = config["DAY_SEGMENTS"]["INCLUDE_PAST_PERIODIC_SEGMENTS"]
    output:
        "data/interim/{pid}/{sensor}_episodes_resampled_with_datetime.csv"
    script:
        "../src/data/readable_datetime.R"

rule phone_application_categories:
    input:
        "data/raw/{pid}/phone_applications_foreground_with_datetime.csv"
    params:
        catalogue_source = config["PHONE_APPLICATIONS_FOREGROUND"]["APPLICATION_CATEGORIES"]["CATALOGUE_SOURCE"],
        catalogue_file = config["PHONE_APPLICATIONS_FOREGROUND"]["APPLICATION_CATEGORIES"]["CATALOGUE_FILE"],
        update_catalogue_file = config["PHONE_APPLICATIONS_FOREGROUND"]["APPLICATION_CATEGORIES"]["UPDATE_CATALOGUE_FILE"],
        scrape_missing_genres = config["PHONE_APPLICATIONS_FOREGROUND"]["APPLICATION_CATEGORIES"]["SCRAPE_MISSING_CATEGORIES"]
    output:
        "data/raw/{pid}/phone_applications_foreground_with_datetime_with_categories.csv"
    script:
        "../src/data/application_categories.R"

rule fitbit_parse_heartrate:
    input:
        "data/raw/{pid}/fitbit_heartrate_{fitbit_data_type}_raw.csv"
    params:
        timezone = config["FITBIT_DATA_CONFIGURATION"]["TIMEZONE"]["VALUE"],
        table = lambda wildcards: config["FITBIT_HEARTRATE_"+str(wildcards.fitbit_data_type).upper()]["TABLE"],
        column_format = config["FITBIT_DATA_CONFIGURATION"]["SOURCE"]["COLUMN_FORMAT"],
        fitbit_data_type = "{fitbit_data_type}"
    output:
        "data/raw/{pid}/fitbit_heartrate_{fitbit_data_type}_parsed.csv"
    script:
        "../src/data/fitbit_parse_heartrate.py"

rule fitbit_parse_steps:
    input:
        "data/raw/{pid}/fitbit_steps_{fitbit_data_type}_raw.csv"
    params:
        timezone = config["FITBIT_DATA_CONFIGURATION"]["TIMEZONE"]["VALUE"],
        table = lambda wildcards: config["FITBIT_STEPS_"+str(wildcards.fitbit_data_type).upper()]["TABLE"],
        column_format = config["FITBIT_DATA_CONFIGURATION"]["SOURCE"]["COLUMN_FORMAT"],
        fitbit_data_type = "{fitbit_data_type}"
    output:
        "data/raw/{pid}/fitbit_steps_{fitbit_data_type}_parsed.csv"
    script:
        "../src/data/fitbit_parse_steps.py"

rule fitbit_parse_sleep:
    input:
        "data/raw/{pid}/fitbit_sleep_{fitbit_data_type}_raw.csv"
    params:
        timezone = config["FITBIT_DATA_CONFIGURATION"]["TIMEZONE"]["VALUE"],
        table = lambda wildcards: config["FITBIT_SLEEP_"+str(wildcards.fitbit_data_type).upper()]["TABLE"],
        column_format = config["FITBIT_DATA_CONFIGURATION"]["SOURCE"]["COLUMN_FORMAT"],
        fitbit_data_type = "{fitbit_data_type}",
        sleep_episode_timestamp = config["FITBIT_SLEEP_SUMMARY"]["SLEEP_EPISODE_TIMESTAMP"]
    output:
        "data/raw/{pid}/fitbit_sleep_{fitbit_data_type}_parsed.csv"
    script:
        "../src/data/fitbit_parse_sleep.py"

# rule fitbit_parse_calories:
#     input:
#         data = expand("data/raw/{{pid}}/fitbit_calories_{fitbit_data_type}_raw.csv", fitbit_data_type = (["json"] if config["FITBIT_CALORIES"]["TABLE_FORMAT"] == "JSON" else ["summary", "intraday"]))
#     params:
#         timezone = config["FITBIT_DATA_CONFIGURATION"]["TIMEZONE"]["VALUE"],
#         table = config["FITBIT_CALORIES"]["TABLE"],
#         table_format = config["FITBIT_CALORIES"]["TABLE_FORMAT"]
#     output:
#         summary_data = "data/raw/{pid}/fitbit_calories_summary_parsed.csv",
#         intraday_data = "data/raw/{pid}/fitbit_calories_intraday_parsed.csv"
#     script:
#         "../src/data/fitbit_parse_calories.py"

rule fitbit_readable_datetime:
    input:
        sensor_input = "data/raw/{pid}/fitbit_{sensor}_{fitbit_data_type}_parsed.csv",
        day_segments = "data/interim/day_segments/{pid}_day_segments.csv"
    params:
        fixed_timezone = config["FITBIT_DATA_CONFIGURATION"]["TIMEZONE"]["VALUE"],
        day_segments_type = config["DAY_SEGMENTS"]["TYPE"],
        include_past_periodic_segments = config["DAY_SEGMENTS"]["INCLUDE_PAST_PERIODIC_SEGMENTS"]
    output:
        "data/raw/{pid}/fitbit_{sensor}_{fitbit_data_type}_parsed_with_datetime.csv"
    script:
        "../src/data/readable_datetime.R"
