package mr.pv;


import common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import value.StatsUserDimension;
import value.map.TimeOutputValue;
import value.reduce.OutputValue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 〈一句话功能简述〉<br>
 * 〈NewUserReducer---reduce方法〉
 * @author 14751
 * @create 2018/9/20
 * @since 1.0.0
 */
public class PageViewReducer extends Reducer<StatsUserDimension,TimeOutputValue,StatsUserDimension, OutputValue> {
    private static final Logger logger = Logger.getLogger(PageViewReducer.class);
    private OutputValue v = new OutputValue();
    //private Set unique = new HashSet();//用于去重，利用HashSet
    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        map.clear();//清空map，因为map是在外面定义的，每一个key都需要调用一次reduce方法，也就是说上次操作会保留map中的key-value
        int  count = 0;
        for(TimeOutputValue tv : values){//循环
            count++;
        }

        //构造输出的value
        //根据kpi别名获取kpi类型（比较灵活） --- 第一种方法
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));

        //这样写比较死，对于每一个kpi都需要进行判断
//        if(key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.NEW_USER.kpiName)){
//            this.v.setKpi(KpiType.NEW_USER);
//        }

        //通过集合的size统计新增用户uuid的个数，前面的key可以随便设置，就是用来标识新增用户个数的（比较难理解）
        this.map.put(new IntWritable(-1),new IntWritable(count));
        this.v.setValue(this.map);
        //输出
        //System.out.println(("Reducer000000000000000000000000000000")+key);
      //  System.out.println(this.unique.size()+"vvvvvvvvvvvvvv");
        context.write(key,this.v);
       // this.unique.clear();//清空操作

        /**
         * 注意点：
         * 如果只是输出到文件系统中，则不需要kpi，不需要声明集合map
         * value只需要uuid的个数，这就不要封装对象了
         */
    }
}
