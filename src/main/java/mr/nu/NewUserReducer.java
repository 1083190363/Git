package mr.nu;

import org.apache.hadoop.mapreduce.Reducer;
import value.StatsUserDimension;
import value.map.TimeOutputValue;
import value.reduce.OutputValue;

import java.io.IOException;

/**
 * @ProjectName: git
 * @Package: mr.nu
 * @ClassName: NewUserReducer
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/3 21:06
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/3 21:06
 * @Version: 1.0
 */
public class NewUserReducer extends Reducer<StatsUserDimension, TimeOutputValue,StatsUserDimension, OutputValue> {
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
