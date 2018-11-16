package mr.hourly;

import IpAnalysis.TimeUtil;
import common.DateEnum;
import common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import value.StatsUserDimension;
import value.map.TimeOutputValue;
import value.reduce.OutputValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @ProjectName: git
 * @Package: mr.activeuser
 * @ClassName: ActiveUserReducer
 * @Description: 活跃用户的reducer类
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/6 22:21
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/6 22:21
 * @Version: 1.0
 */
public class HourlyActiveUserReducer extends Reducer<StatsUserDimension, TimeOutputValue,StatsUserDimension, OutputValue> {
    private static final Logger logger = Logger.getLogger(HourlyActiveUserReducer.class);
    private OutputValue v = new OutputValue();
    private Set unique = new HashSet();//用于去重，利用HashSet
    private MapWritable map = new MapWritable();
    private Map<Integer,Set<String>> hourlyMap = new HashMap<Integer,Set<String>>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //循环初始化
        for (int i=0;i<24;i++) {
            hourlyMap.put(i,new HashSet<String>());
            map.put(new IntWritable(),new IntWritable(0));
        }
    }
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
       try{
           //判断事件是哪一个
           String kpi = key.getStatsCommonDimension().getKpiDimension().getKpiName();
           if (kpi.equals(KpiType.HOURLY_ACTIVE_USER)){
               for (TimeOutputValue tv:values) {
                   int hour = TimeUtil.getDateInfo(tv.getTime(), DateEnum.HOUR);
                   hourlyMap.get(hour).add(tv.getId());
               }
               //构建输出的value
               this.v.setKpi(KpiType.HOURLY_ACTIVE_USER);
               //循环赋值
               for (Map.Entry<Integer,Set<String>> en:hourlyMap.entrySet()) {
                   this.map.put(new IntWritable(en.getKey()),
                           new IntWritable(en.getValue().size()));
               }
               this.v.setValue(map);
               //输出
               context.write(key,this.v);
           }else {
               for (TimeOutputValue tv : values) {
                   //将uuid取出来添加到set中
                   this.unique.add(tv.getId());
               }
               //构造输出的value
               MapWritable map1 = new MapWritable();
               map1.put(new IntWritable(-1),new IntWritable(this.unique.size()));
               this.v.setValue(this.map);
               this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
               //输出
               context.write(key,this.v);
           }
       }finally {
           this.unique.clear();
           this.hourlyMap.clear();
           this.map.clear();
           //循环初始化
           for (int i = 0;i<24;i++){
               hourlyMap.put(i,new HashSet<String>());
               map.put(new IntWritable(i),new IntWritable(0));
           }
       }
    }
}
