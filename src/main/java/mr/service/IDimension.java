package mr.service;

import dimension.base.BaseDimension;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @ProjectName: git
 * @Package: mr.service
 * @ClassName: IDimension
 * @Description: 根据维度获取对应的Id的接口
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/3 21:03
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/3 21:03
 * @Version: 1.0
 */
public interface IDimension {
    int getDimensionIdByObject(BaseDimension dimension) throws IOException, SQLException;
}
