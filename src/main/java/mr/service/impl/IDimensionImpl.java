package mr.service.impl;

import dimension.base.BaseDimension;
import mr.service.IDimension;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @ProjectName: git
 * @Package: mr.service.impl
 * @ClassName: IDimensionImpl
 * @Description: 获取基础维度Id的实现
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/3 21:02
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/3 21:02
 * @Version: 1.0
 */
public class IDimensionImpl implements IDimension {
    //定义内存缓存，用来缓存维度--维度Id
    private Map<String,Integer> cache = new LinkedHashMap<String,Integer>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 5000;
        }
    };

    //1、根据维度对象里面的属性值，赋值给对应的sql，然后查询，如果有则返回对应维度的Id
    //  如果没有，则先添加到数据库中然后返回新增的Id号
    @Override
    public int getDimensionIdByObject(BaseDimension dimension) throws IOException, SQLException {
        return 0;
    }
}