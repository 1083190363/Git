package dimension.base;

import java.sql.PreparedStatement;

/**
 * @ProjectName: git
 * @Package: dimension.base
 * @ClassName: BrowserActionDimension
 * @Description: java类作用描述
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/5 14:58
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/5 14:58
 * @Version: 1.0
 */
public class BrowserActionDimension extends BaseActionDimension{
    @Override
    public String buildCacheKey() {
        return null;
    }

    @Override
    public String buildSqls() {
        return null;
    }

    @Override
    public void setArgs(PreparedStatement ps) {

    }
}
