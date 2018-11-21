package dimension.hive.order;

import common.GlobalConstants;
import dimension.base.PaymentTypeDimension;
import mr.service.IDimensionConverter;
import mr.service.impl.IDimensionImplConverter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @ProjectName: git
 * @Package: dimension.hive.order
 * @ClassName: PaymentTypeDimensionUDF
 * @Description: 支付方式的UDF
 * @Author: CaoXueCheng
 * @CreateDate: 2018/11/20 11:38
 * @UpdateUser: CaoXueCheng
 * @UpdateDate: 2018/11/20 11:38
 * @Version: 1.0
 */
public class PaymentTypeDimensionUDF extends UDF {
    private IDimensionConverter converter = new IDimensionImplConverter();
    public int evaluate(String payment_type){
     if (StringUtils.isEmpty(payment_type)){
         payment_type = GlobalConstants.DEFAULT_VALUE;
     }
     int id = -1;
     PaymentTypeDimension paymentType = new PaymentTypeDimension(payment_type);
        try {
            id = converter.getDimensionIdByObject(paymentType);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new PaymentTypeDimensionUDF().evaluate("alipay"));
    }
}
