select childid
	   , max(readingage) as readingage
from events_live
where (clientversion >='3.6.0' or CLIENTVERSION like '3.10.%')
		and gauserstartdate >= '2023-01-18'
		and readingage is not null
group by childid, readingage