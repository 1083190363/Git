package mr.newmember;

import common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import value.StatsUserDimension;
import value.map.TimeOutputValue;
import value.reduce.OutputValue;

import java.io.IOException;
import java.util.*;

/**
 * @ProjectName: git
 * @Package: mr.newmemeber
 * @ClassName: NewMemeberReducer
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/9 19:18
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/9 19:18
 * @Version: 1.0
 */
public class NewMemeberReducer extends Reducer<StatsUserDimension, TimeOutputValue,StatsUserDimension, OutputValue> {
    private static final Logger logger = Logger.getLogger(NewMemeberReducer.class);
    private OutputValue v = new OutputValue();
    //private Set unique = new HashSet();//用于去重，利用HashSet
    private MapWritable map = new MapWritable();
    private Map<String, List<Long>> li = new HashMap<String,List<Long>>();
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空上次的key_value
        map.clear();
        for (TimeOutputValue tv:values) {
//            this.unique.add(en.getId());
                /*this.unique.add(tv.getId());//将mid取出添加到set中进行去重操作
            this.li.add(tv.getTime());*/
            if(li.containsKey(tv.getId())){
                li.get(tv.getId()).add(tv.getTime());
            } else {
                List<Long> list = new ArrayList<Long>();
                list.add(tv.getTime());
                li.put(tv.getId(),list);
            }
        }
        //循环输出  用于插入到member_info表中
        for (Map.Entry<String,List<Long>> en:li.entrySet()){
            this.v.setKpi(KpiType.MEMBER_INFO);
            this.map.put(new IntWritable(-2),new Text(en.getKey()));
            Collections.sort(en.getValue());
            this.map.put(new IntWritable(-3),new LongWritable(en.getValue().get(0)));
            this.v.setValue(this.map);
            context.write(key,this.v);
        }
        //构造输出的value
        //根据kpi别名获取kpi类型（比较灵活） --- 第一种方法
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        this.map.put(new IntWritable(-1),new IntWritable(this.li.size()));
        this.v.setValue(this.map);
        context.write(key,this.v);
       // this.unique.clear();
    }
}
