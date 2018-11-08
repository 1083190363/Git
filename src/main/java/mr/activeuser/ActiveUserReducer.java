package mr.activeuser;

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
public class ActiveUserReducer extends Reducer<StatsUserDimension, TimeOutputValue,StatsUserDimension, OutputValue> {
    private static final Logger logger = Logger.getLogger(ActiveUserReducer.class);
    private OutputValue v = new OutputValue();
    private Set unique = new HashSet();//用于去重，利用HashSet
    private MapWritable map = new MapWritable();
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空上次的key_value
        map.clear();
        for (TimeOutputValue en:values) {
            this.unique.add(en.getId());
        }
        //构造输出的value
        //根据kpi别名获取kpi类型（比较灵活） --- 第一种方法
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        this.v.setValue(this.map);
       // System.out.println(v+"++++++++++++");
        context.write(key,this.v);
        this.unique.clear();
    }
}
