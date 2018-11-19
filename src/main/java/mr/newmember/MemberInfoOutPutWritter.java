package mr.newmember;

import common.GlobalConstants;
import mr.IOutputWritter;
import mr.service.IDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import value.StatsBaseDimension;
import value.StatsOutputValue;
import value.StatsUserDimension;
import value.reduce.OutputValue;

import java.sql.PreparedStatement;

/**
 * @ProjectName: git
 * @Package: mr.newmemeber
 * @ClassName: NewMemberOutPutWritter
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/9 19:19
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/9 19:19
 * @Version: 1.0
 */
public class MemberInfoOutPutWritter implements IOutputWritter {
    private static final Logger logger = Logger.getLogger(MemberInfoOutPutWritter.class);
    @Override
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputValue v = (OutputValue)value;
            //计算活跃会员的值
            String memberId = ((Text)(v.getValue().get(new IntWritable(-2)))).toString();
            int i = 0;
            long minTime = ((LongWritable)(v.getValue().get(new IntWritable(-3)))).get();
            ps.setString(++i,memberId);
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.setLong(++i,minTime);//
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.addBatch();
        } catch (Exception e) {
            logger.warn("ps赋值异常");
        }
    }
}
