TRUNCATE TABLE IF EXISTS AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;
use role sysadmin;


-- Create The Same Raw Table for the automated pipeline version of work
create or replace TABLE AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS (
	RAW_LOG VARIANT
);

-- Create The Same View for the automated pipeline version of work
create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS(
	IP_ADDRESS,
	USER_EVENT,
	USER_LOGIN,
	DATETIME_ISO8601,
	RAW_LOG
) as (
select 
    RAW_LOG:ip_address::text as IP_ADDRESS
    ,RAW_LOG:user_event::text as USER_EVENT
    ,RAW_LOG:user_login::text as USER_LOGIN
    ,RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as DATETIME_ISO8601
    ,*
from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
where RAW_LOG:agent::text is null

);


copy into AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
from @ags_game_audience.raw.uni_kishore_pipeline
file_format = (format_name=ags_game_audience.raw.FF_JSON_LOGS);

merge into AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
using (
SELECT logs.ip_address
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as GAMER_LTZ_NAME
, dayname(GAMER_LTZ_NAME) as DOW_NAME
, tod_name as TOD_NAME

FROM AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
JOIN IPINFO_GEOLOC.DEMO.LOCATION loc
ON IPINFO_GEOLOC.PUBLIC.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.PUBLIC.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN AGS_GAME_AUDIENCE.RAW.time_of_day_lu lu
ON DATE_PART(hour, GAMER_LTZ_NAME) = lu.hour

) r

ON r.gamer_name = e.gamer_name 
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, TIMEZONE, GAMER_LTZ_NAME, DOW_NAME, TOD_NAME
) VALUES (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, TIMEZONE, GAMER_LTZ_NAME, DOW_NAME, TOD_NAME);


create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
schedule='5 Minutes'
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
as
copy into AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
from @ags_game_audience.raw.uni_kishore_pipeline
file_format = (format_name=ags_game_audience.raw.FF_JSON_LOGS);

create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
as
merge into AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
using (
SELECT logs.ip_address
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone
, convert_timezone('UTC', timezone, logs.datetime_iso8601) as GAMER_LTZ_NAME
, dayname(GAMER_LTZ_NAME) as DOW_NAME
, tod_name as TOD_NAME

FROM AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
JOIN IPINFO_GEOLOC.DEMO.LOCATION loc
ON IPINFO_GEOLOC.PUBLIC.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.PUBLIC.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN AGS_GAME_AUDIENCE.RAW.time_of_day_lu lu
ON DATE_PART(hour, GAMER_LTZ_NAME) = lu.hour

) r

ON r.gamer_name = e.gamer_name 
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name
WHEN NOT MATCHED THEN
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, TIMEZONE, GAMER_LTZ_NAME, DOW_NAME, TOD_NAME
) VALUES (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, TIMEZONE, GAMER_LTZ_NAME, DOW_NAME, TOD_NAME);


-- check the number of rows so that if something weird happens, data engineer will recognize it sooner. 
select * from AGS_GAME_AUDIENCE.RAW.PL_LOGS;
--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step 3 - number of rows in raw view (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay because not all IP addresses are available from the IPInfo share)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


-- DO NOT EDIT THIS CODE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW05' as step
 ,(
   select max(tally) from (
       select CASE WHEN SCHEDULED_FROM = 'SCHEDULE' 
                         and STATE= 'SUCCEEDED' 
              THEN 1 ELSE 0 END as tally 
   from table(ags_game_audience.information_schema.task_history (task_name=>'GET_NEW_FILES')))
  ) as actual
 ,1 as expected
 ,'Task succeeds from schedule' as description
 ); 

