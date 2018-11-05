package mr;

/**
 * @ProjectName: git
 * @Package: mr
 * @ClassName: IOutputWritter
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/5 14:31
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/5 14:31
 * @Version: 1.0
 */

import mr.service.IDimension;
import org.apache.hadoop.conf.Configuration;
import value.StatsBaseDimension;
import value.StatsOutputValue;

import java.sql.PreparedStatement;

/**
 * 操作结果表的接口
 */
public interface IOutputWritter {

    /**
     * 为每一个kpi的最终结果赋值的接口
     * @param conf
     * @param key
     * @param value
     * @param ps
     * @param iDimension
     */
    void output(Configuration conf,
                StatsBaseDimension key,
                StatsOutputValue value,
                PreparedStatement ps,
                IDimension iDimension);
}
