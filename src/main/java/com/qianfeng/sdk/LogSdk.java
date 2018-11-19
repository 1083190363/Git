package com.qianfeng.sdk;


import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LogSdk {
    //日志打印对象
    private static final Logger logger= Logger.getGlobal();
    //定义常量
    private static final String ver = "1.0";
    private static String platforName="java_server";
    private static String chargeSuccess="e_cs";
    private static String chargeRefund="e_cr";
    private static String sdkName="java_sdk";
    private static String requestUrl="http://192.168.137.6";

    /**
     *
     * @param mid
     * @param oid
     * @param flag 1 chargeSuccess 2chargeRefund  默认使用1
     * @return
     */
    public static String chargeSuccess(String mid,String oid,String flag) throws UnsupportedEncodingException {
        if (isEmpty(mid) || isEmpty(oid)){
            logger.log(Level.WARNING,"mid or oid is null");
          // return false;
        }

        //mid oid 不为空
        Map<String,String> data = new HashMap<String, String>();
        if (isEmpty(flag) || flag.equals("1")){
            data.put("en",chargeSuccess);
        }else if (flag.equals("2")){
            data.put("en",chargeRefund);
        }
        data.put("pl",platforName);
        data.put("sdk",sdkName);
        data.put("c_time",System.currentTimeMillis()+"");
        data.put("ver",ver);
        data.put("u_mid",mid);
        data.put("oid",oid);
        //构造请求的url
        String url = buildUrl(data);
       //将url添加到队列中
       SendUrl.getInstance().addUrlQuene(url);
       String json = "{\"code\":200,\"data\":{\"isSuccess\":true}}";
        //return true ;
        return json;
    }



    private static String buildUrl(Map<String, String> data) throws UnsupportedEncodingException {
        if (data.isEmpty()){
            return null;
        }
        StringBuffer sb = new StringBuffer();
        sb.append(requestUrl).append("?");
        //循环data
        for (Map.Entry<String,String> en:data.entrySet()){
            if (isNotEmpty(en.getKey())){
                sb.append(en.getKey()).append("=").append(
                        URLEncoder.encode(en.getValue(),"utf-8")
                ).append("&");
            }
        }
        return sb.toString().substring(0,sb.length()-1);
    }

    /**
     * 判断字符串是否为空.为空返回true,否则返回false
     * @param input
     * @return
     */
    public static boolean isEmpty(String input){
       return input==null || input.trim().equals("")||
               input.trim().length()==0?true:false;
    }
    public static boolean isNotEmpty(String input){
       return !isEmpty(input);
    }
    public static void main(String[] args) {
        System.out.println(isEmpty("aaaaa"));
    }
}
