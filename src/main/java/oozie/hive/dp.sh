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
#正式运行sql
hive --database pv -e "
with tmp as(
select pl as pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plass from dwa_dp where col = 'pv1' union all
select pl as pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plass from dwa_dp where col = 'pv2' union all
select pl as pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plass from dwa_dp where col = 'pv3' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plass from dwa_dp where col = 'pv4' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plass from dwa_dp where col = 'pv5_10' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60plass from dwa_dp where col = 'pv10_30' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60plass from dwa_dp where col = 'pv30_60' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60plass from dwa_dp where col = 'pv60plass' union all
select 'all' as pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plass from dwa_dp where col = 'pv1' union all
select 'all' as pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plass from dwa_dp where col = 'pv2' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plass from dwa_dp where col = 'pv3' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plass from dwa_dp where col = 'pv4' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plass from dwa_dp where col = 'pv5_10' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60plass from dwa_dp where col = 'pv10_30' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60plass from dwa_dp where col = 'pv30_60' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60plass from dwa_dp where col = 'pv60plass'
)
from tmp
insert overwrite table stats_view_depth
select date_convert(dt),platform_convert(pl),2,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60plass),dt
group by pl,dt
;
"
#运行sqoop
echo "run sqoop statement"
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username hadoop --password hadoop --table stats_view_depth \
--export-dir hdfs://hadoop01:9000/user/hive/warehouse/pv.db/stats_view_depth/* \
--input-fields-terminated-by '\001' \
--update-key platform_dimension_id,date_dimension_id,kpi_dimension_id \
--update-mode allowinsert \
;