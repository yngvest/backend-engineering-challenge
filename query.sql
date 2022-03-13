with data as (select
                  JSONExtractUInt(json, 'duration') as dur,
                  parseDateTimeBestEffort(JSONExtractString(json, 'timestamp')) as ts
              from table),
     minmax as (select toStartOfMinute(max(ts)) as mx, toStartOfMinute(min(ts)) as mn from data),
     tsRange as (select FROM_UNIXTIME(arrayJoin(range (toUnixTimestamp(mn), toUnixTimestamp(mx) + 120, 60))) as t from minmax),
     {ws:UInt16} as wsize
select t as "date", if(not isNaN(a), avgIf(dur, ts between addMinutes(t, -wsize) and t) as a, 0) as "average_delivery_time"
from tsRange
left join data
on 1 -- should join on 'ts between addMinutes(t, -wsize) and t' but clickhouse doesn't support it :(
group by t
order by t
