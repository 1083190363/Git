package dimension.base;

import java.sql.PreparedStatement;

/**
 * @ProjectName: git
 * @Package: dimension.base
 * @ClassName: BaseActionDimension
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/5 14:57
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/5 14:57
 * @Version: 1.0
 */
public abstract class BaseActionDimension {
    protected static BaseDimension dimension;

    public abstract  String buildCacheKey();

    public  abstract String buildSqls();

    public abstract  void setArgs(PreparedStatement ps);
}