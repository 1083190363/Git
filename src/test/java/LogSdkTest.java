import com.qianfeng.sdk.LogSdk;

import java.io.UnsupportedEncodingException;

/**
 * 测试类
 */
public class LogSdkTest {

    public static void main(String[] args) {
        try {
            System.out.println(LogSdk.chargeSuccess("zhangsan-999"
                    ,"123460","2"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
