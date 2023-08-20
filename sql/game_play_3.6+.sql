with adjusted_date_times as (
  SELECT *
    , CASE WHEN LENGTH(timezoneoffset) > 0 THEN
    DATEADD('minute', CAST(SUBSTR(timezoneoffset, 1, 3) * 60 + SUBSTR(timezoneoffset, 4, 2) AS INTEGER), eventtimestamp)
    ELSE
      eventtimestamp
    END AS local_timestamp -- use timezoneoffset to create local_timestamp
    , TO_DATE(DATE_TRUNC('DAY', local_timestamp)) AS local_eventdate -- create local_eventdate from local_timestamp
    , min(local_eventdate) over (partition by userid) as local_startdate --  create local_startdate from local_eventdate
FROM events_live
where eventname = 'pnsCompleted' 
		  and pnsmode not like 'Pop%' 
		  and gauserstartdate >= '2023-01-18'
		  and (clientversion >= '3.6.0' or clientversion like '3.10.%') 
) 
select * 
from (
	select userid
		   , childid
		   , sessionid
		   , local_eventdate
 		   , local_timestamp
		   , local_startdate
		   , gausercountry
		   , clientversion
		   , pnsmode
		   , pnsword
		   , pnsduration
		   , pnsmistakes
		   , row_number() over (partition by childid, pnsword order by local_timestamp) as exposures
		   , local_eventdate - local_startdate as day_since_start
	from adjusted_date_times
	where childid in (
		select distinct childid
		from (
			select *
				   , row_number() over (partition by childid, pnsword order by local_timestamp) as exposures
				   , DATEDIFF('DAY', local_timestamp::TIMESTAMP_NTZ, CURRENT_TIMESTAMP::TIMESTAMP_NTZ) AS diff_from_current_day
				   , local_eventdate - local_startdate as day_since_start
			from adjusted_date_times	
			order by childid, pnsword, exposures desc
			)
		where exposures = 8
			  and pnsmode = 'MatchSentenceBlank'
			  )
	order by userid, childid, eventtimestamp
	)
where exposures between 1 and 8;



-- with completed as (
-- 	select * 
-- 	from events_live 
-- 	where eventname = 'pnsCompleted' 
-- 		  and pnsmode not like 'Pop%' 
-- 		  and gauserstartdate >= '2023-01-18'
-- 		  and (clientversion >= '3.6.0' or clientversion like '3.10.%')
-- ) 
-- select * 
-- from (
-- 	select userid
-- 		   , childid
-- 		   , sessionid
-- 		   , eventdate
-- 		   , eventtimestamp
-- 		   , gauserstartdate
-- 		   , gausercountry
-- 		   , clientversion
-- 		   , pnsmode
-- 		   , pnsword
-- 		   , pnsduration
-- 		   , pnsmistakes
-- 		   , row_number() over (partition by childid, pnsword order by eventtimestamp) as exposures
-- 		   , eventdate - gauserstartdate as day_since_start
-- 	from completed
-- 	where childid in (
-- 		select distinct childid
-- 		from (
-- 			select *
-- 				   , row_number() over (partition by childid, pnsword order by eventtimestamp) as exposures
-- 				   , DATEDIFF('DAY', eventtimestamp::TIMESTAMP_NTZ, CURRENT_TIMESTAMP::TIMESTAMP_NTZ) AS diff_from_current_day
-- 				   , eventdate - gauserstartdate as day_since_start
-- 			from completed	
-- 			order by childid, pnsword, exposures desc
-- 			)
-- 		where exposures = 8
-- 			  and pnsmode = 'MatchSentenceBlank'
-- 			  )
-- 	order by userid, childid, eventtimestamp
-- 	)
-- where exposures between 1 and 8;