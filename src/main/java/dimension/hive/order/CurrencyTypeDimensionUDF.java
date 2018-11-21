package dimension.hive.order;

import common.GlobalConstants;
import dimension.base.CurrencyTypeDimension;
import mr.service.IDimensionConverter;
import mr.service.impl.IDimensionImplConverter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.io.IOException;
import java.sql.SQLException;

/**
 * @ProjectName: git
 * @Package: dimension.hive.order
 * @ClassName: CurrencyTypeDimensionUDF
 * @Description: 支付货币类型的UDF
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/20 11:37
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/20 11:37
 * @Version: 1.0
 */
public class CurrencyTypeDimensionUDF extends UDF {
    private IDimensionConverter converter = new IDimensionImplConverter();
    public int evaluate(String currencyType){
        if (StringUtils.isEmpty(currencyType)){
            currencyType = GlobalConstants.DEFAULT_VALUE;
        }
        CurrencyTypeDimension currency= new CurrencyTypeDimension(currencyType);
        int id = -1;
        try {
            id = converter.getDimensionIdByObject(currency);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new CurrencyTypeDimensionUDF().evaluate("$"));
    }
}
