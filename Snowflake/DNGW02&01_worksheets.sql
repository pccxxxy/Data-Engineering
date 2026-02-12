-- Exploring the File Before Loading It
select $1
from @uni_kishore/kickoff
(file_format => FF_JSON_LOGS);

-- Load the File Into The Table
copy into ags_game_audience.raw.game_logs
from @uni_kishore/kickoff
file_format = (format_name=FF_JSON_LOGS)

-- Separates Every Attribute into Its Own Column
select
    RAW_LOG:agent::text as AGENT
    ,RAW_LOG:user_event::text as USER_EVENT
    ,RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as DATETIME_ISO8601
    ,RAW_LOG:user_login::text as USER_LOGIN
    ,* 
from game_logs;

-- Save a Select Statement for future use by View Statement
create view ags_game_audience.raw.logs as (select
    RAW_LOG:agent::text as AGENT
    ,RAW_LOG:user_event::text as USER_EVENT
    ,RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as DATETIME_ISO8601
    ,RAW_LOG:user_login::text as USER_LOGIN
    ,* 
from game_logs);

select * from logs;


-- DO NOT EDIT THIS CODE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DNGW01' as step
  ,(
      select count(*)  
      from ags_game_audience.raw.logs
      where is_timestamp_ntz(to_variant(datetime_iso8601))= TRUE 
   ) as actual
, 250 as expected
, 'Project DB and Log File Set Up Correctly' as description
); 




-- Load the Updated File Into The Table
copy into ags_game_audience.raw.game_logs
from @uni_kishore/updated_feed
file_format = (format_name=FF_JSON_LOGS);


-- Update Your LOGS View so that it accommdates to the new file 
create or replace view ags_game_audience.raw.logs as (
select 
    RAW_LOG:ip_address::text as IP_ADDRESS
    ,RAW_LOG:user_event::text as USER_EVENT
    ,RAW_LOG:user_login::text as USER_LOGIN
    ,RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as DATETIME_ISO8601
    ,*
from ags_game_audience.raw.game_logs
where RAW_LOG:ip_address::text is not null

);


-- DO NOT EDIT THIS CODE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
   'DNGW02' as step
   ,( select sum(tally) from(
        select (count(*) * -1) as tally
        from ags_game_audience.raw.logs 
        union all
        select count(*) as tally
        from ags_game_audience.raw.game_logs)     
     ) as actual
   ,250 as expected
   ,'View is filtered' as description
); 