#!/bin/bash
#判断时间格式
#./en.sh -n -m -d 2018-08-30
run_date=
until [ $# -eq 0 ]
do
if [ $1'x' = '-dx' ]
then
shift
run_date=$1
fi
shift
done


if [ ${#run_date} != 10 ]
then
run_date=`date -d "1 days ago" "+%Y-%m-%d"`
else
echo "$run_date"
fi

month=`date -d "$run_date" "+%m"`
day=`date -d "$run_date" "+%d"`
echo "final running date is:${run_date},${month},${day}"
#正式运行hql
hive --database pv -e "
with tmp as(
select
from_unixtime(cast(d.s_time/1000 as bigint),"yyyy-MM-dd") dt,
d.pl pl,d.ca ca,d.ac ac,count(1) count
from dw_en d
where d.pl is not null
and d.month = "${month}"
and d.day = "${day}"
group by d.s_time,d.pl,d.ca,d.ac
)
from(
select pl as pl,dt as dt,ca as ca,ac as ac,count(1)as ct from tmp group by pl,dt,ca,ac union all
select pl as pl,dt as dt,ca as ca,'all' as ac,count(1) as ct from tmp group by pl,dt,ca union all
select pl as pl,dt as dt,'all' as ca,'all' as ac,count(1) as ct from tmp group by pl,dt union all
select 'all' as pl,dt as dt,ca as ca,ac as ac,count(1) as ct from tmp group by dt,ca,ac union all
select 'all' as pl,dt as dt,ca as ca,'all' as ac,count(1) as ct from tmp group by dt,ca union all
select 'all' as pl,dt as dt,'all' as ca,'all' as ac,count(1) as ct from tmp group by dt
)as tmp2
insert into stats_event
select date_convert(dt),platform_convert(pl),event_convert(ca,ac),sum(ct),dt
group by pl,dt,ca,ac
;
"

echo "run sqoop statment"
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username hadoop --password hadoop --table stats_event \
--export-dir hdfs://hadoop01:9000/user/hive/warehouse/pv.db/stats_event/* \
--input-fields-terminated-by '\001' \
--update-key platform_dimension_id,date_dimension_id,event_dimension_id \
--update-mode allowinsert \
;

echo "event job is finished"


