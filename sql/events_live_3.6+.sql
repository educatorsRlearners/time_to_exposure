select
    userid
    ,childid
    ,eventname
    ,eventtimestamp
    ,mssincelastevent
    ,timezoneoffset
    ,gausercountry
    ,clientversion
    ,pnsmode
    ,pnsword
    ,pnsduration
    ,pnsmistakes
from events_live
where (clientversion >= '3.6' or clientversion like '3.10.%')
    and gauserstartdate >= '2023-01-18'
    and userid in ('000ad428-f561-4586-b6bb-6587e6e39832')