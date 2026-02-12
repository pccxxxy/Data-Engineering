create or replace table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS (
    log_file_name VARCHAR(100), --new metadata column
    log_file_row_id NUMBER(18,0), --new metadata column
    load_ltz TIMESTAMP_LTZ(0),--new local time of load
    DATETIME_ISO8601 TIMESTAMP_NTZ(9),
    USER_EVENT VARCHAR(25),
    USER_LOGIN VARCHAR(100),
    IP_ADDRESS VARCHAR(100)  
);

-- Exploring the File Before Loading It
select $1
from @uni_kishore/kickoff
(file_format => FF_JSON_LOGS);

COPY INTO AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = FF_JSON_LOGS);

