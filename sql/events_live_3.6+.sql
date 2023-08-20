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
    and userid in (
                '000ad428-f561-4586-b6bb-6587e6e39832'
                , '00126038-0fd5-47a1-aced-39406c35fc0d'
                , '00355778-afa1-4d23-bf22-5968bdd46509')
-- limit 400000