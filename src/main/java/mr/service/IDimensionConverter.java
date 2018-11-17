package mr.service;

import java.io.IOException;
import java.sql.SQLException;

import dimension.base.BaseDimension;

/**
 * 专门提供操作dimesion表的接口
 * @author lyd
 *
 */
public interface IDimensionConverter {
	/**
	 * 根据dimesion的value值获取对应的id
	 * 如果数据库中有，则直接返回对应的value的id；如果没有，则先插入，再返回插入行value的id
	 * @param
	 * @return
	 * @throws IOException
	 */
	int getDimensionIdByObject(BaseDimension dimension) throws IOException, SQLException;
}
