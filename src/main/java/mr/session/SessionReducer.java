package mr.session;

import common.GlobalConstants;
import common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import value.StatsUserDimension;
import value.map.TimeOutputValue;
import value.reduce.OutputValue;
import java.io.IOException;
import java.util.*;

/**
 * @ProjectName: git
 * @Package: mr.session
 * @ClassName: SessionReducer
 * @Description: 活跃用户的reducer类
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/6 22:21
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/6 22:21
 * @Version: 1.0
 */
public class SessionReducer extends Reducer<StatsUserDimension, TimeOutputValue,StatsUserDimension, OutputValue> {
    private static final Logger logger = Logger.getLogger(SessionReducer.class);
    private OutputValue v = new OutputValue();
    //private Set unique = new HashSet();//用于去重，利用HashSet
    private MapWritable map = new MapWritable();
    private Map<String, List<Long>> sessionAndTime = new HashMap<String, List<Long>>();
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空上次的key_value
        this.sessionAndTime.clear();
        for (TimeOutputValue en:values) {
            String sessionId = en.getId();
            long serverTime = en.getTime();
            //存储时间
            if (sessionAndTime.containsKey(en.getId())){
                List<Long> list = sessionAndTime.get(sessionId);
                list.add(serverTime);
                sessionAndTime.put(sessionId,list);
            }else {
                List<Long> li = new ArrayList<>();
                li.add(serverTime);
                sessionAndTime.put(sessionId,li);
            }

        }
        //构造输出的value
        //根据kpi别名获取kpi类型（比较灵活） --- 第一种方法
        this.map.put(new IntWritable(-1),new IntWritable(this.sessionAndTime.size()));
        //session的时长
        int sessionLength = 0;
        for (Map.Entry<String,List<Long>> en: sessionAndTime.entrySet()) {
            if (en.getValue().size() >=2){
               Collections.sort(en.getValue());
                sessionLength += (en.getValue().get(en.getValue().size() - 1) - en.getValue().get(0));
            }
        }
        if (sessionLength > 0 && sessionLength <= GlobalConstants.DAY_OF_MILLSECOND){
            if (sessionLength % 1000 == 0){
                sessionLength = sessionLength / 1000;
            }else {
                sessionLength = sessionLength /1000+1;

            }
        }
        this.map.put(new IntWritable(-2),new IntWritable(sessionLength));
        this.v.setValue(this.map);
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        context.write(key,this.v);
        //this.unique.clear();
    }
}
