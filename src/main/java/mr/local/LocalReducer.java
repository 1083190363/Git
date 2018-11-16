package mr.local;

import common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import value.StatsUserDimension;
import value.StatusLocalDimension;
import value.map.TextOutputValue;
import value.map.TimeOutputValue;
import value.reduce.LocalOutputValue;
import value.reduce.OutputValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @ProjectName: git
 * @Package: mr.activemember
 * @ClassName: ActiveMemberReducer
 * @Description: 地域维度的reducer类
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/8 15:36
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/8 15:36
 * @Version: 1.0
 */
public class LocalReducer extends Reducer<StatusLocalDimension, TextOutputValue,StatusLocalDimension, LocalOutputValue> {
    private static final Logger logger = Logger.getLogger(LocalReducer.class);
    //uuid个数
    private Set unique = new HashSet();//用于去重，利用HashSet
    private Map<String,Integer> map = new HashMap<String, Integer>();
    LocalOutputValue v = new LocalOutputValue();
    @Override
    protected void reduce(StatusLocalDimension key, Iterable<TextOutputValue> values, Context context) throws IOException, InterruptedException {
        this.map.clear();
        this.unique.clear();
        for (TextOutputValue value:values) {
            this.unique.add(value.getId());
            if (map.containsKey(value.getSessionid())){
                map.put(value.getSessionid(),2);
            }else {
                map.put(value.getSessionid(),1);
            }
        }
        this.v.setAus(this.unique.size());
        this.v.setSessions(this.map.size());
        int boundSessions = 0;
        for (Map.Entry<String,Integer> en : map.entrySet()){
            if (en.getValue() < 2){
                boundSessions++;
            }
        }
        this.v.setBoundsessions(boundSessions);
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        context.write(key,v);
    }
}
