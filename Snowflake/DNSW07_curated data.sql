--Gamer cities
select distinct gamer_name, city
from ags_game_audience.enhanced.logs_enhanced_bu;

--Time of day
select tod_name as time_of_day
           , count(*) as tally
     from ags_game_audience.enhanced.logs_enhanced_bu
     group by  tod_name
     order by tally desc;   

--Session Length versus Time of Day
--correlation betweent the total amount of time each gamer played and time of day
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length < 20 then '10 to 19 mins'
            when game_session_length < 30 then '20 to 29 mins'
            when game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins' 
            end as session_length
            ,tod_name
from (
select GAMER_NAME
       , tod_name
       ,GAMER_LTZ_NAME as login 
       ,lead(GAMER_LTZ_NAME) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAMER_LTZ_NAME
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_BU)
where logout is not null;




-- DO NOT EDIT THIS CODE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW07' as step
 ,( select count(*)/count(*) from snowflake.account_usage.query_history
    where query_text like '%case when game_session_length < 10%'
  ) as actual
 ,1 as expected
 ,'Curated Data Lesson completed' as description
 ); 
