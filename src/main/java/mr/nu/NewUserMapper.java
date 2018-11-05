package mr.nu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import value.StatsUserDimension;
import value.map.TimeOutputValue;

import java.io.IOException;

/**
 * @ProjectName: git
 * @Package: mr.nu
 * @ClassName: NewUserMapper
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/3 21:05
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/3 21:05
 * @Version: 1.0
 */
public class NewUserMapper extends Mapper<LongWritable, Text, StatsUserDimension, TimeOutputValue> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
