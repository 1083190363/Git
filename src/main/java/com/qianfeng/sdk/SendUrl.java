package com.qianfeng.sdk;


import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 用于发送已经构建好的url
 */
public class SendUrl {
    //日志打印对象
    private static final Logger logger= Logger.getGlobal();
    //定义一个存储url的队列
    private static final BlockingQueue<String> queue =
            new LinkedBlockingQueue<>();
    //创建单例对象
    private static SendUrl senderUrl= null;
    //私有的构造器
    private SendUrl(){
    }
    //公有的获取该类实例的方法
    public static SendUrl getInstance(){
        //先判断senderUrl是否为空
        if (senderUrl == null){
            //防止同时有两个线程同时来获取
            synchronized (SendUrl.class){
                if (senderUrl == null){
                    senderUrl = new SendUrl();
                    //创建一个独立线程
                    Thread th = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            //TODO
                            try {
                                SendUrl.senderUrl.sendUrl1();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                    //如果需要挂载启动
                   // th.setDaemon(true);建议在服务器运行时可以挂载启动
                    //启动线程
                    th.start();
                }
            }
        }
        return senderUrl;
    }

    /**
     * 将url添加到自己的发送队列中
     */
    public static void addUrlQuene(String url){
        try {
            getInstance().queue.put(url);
            //getInstance().queue.add(url);
        } catch (Exception e) {
            logger.log(Level.WARNING,"添加url到队列中异常");
        }
    }

    public static void sendUrl1() throws InterruptedException, IOException {
        while (true){
            String url = queue.take();
            HttpUtil.sendUrl(url);//发送
        }
    }
    /**
     * 用于发送url的工具类
     */
    public static class HttpUtil{
        /**
         * 发送url
         */
        public static void sendUrl(String url) throws IOException {
            HttpURLConnection conn = null;
            InputStream is = null;
            //构建url
            URL url1 = new URL(url);
            //获取url
            conn = (HttpURLConnection) url1.openConnection();
            //为conn设置属性
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            //真正发送
           is = conn.getInputStream();
            conn.disconnect();
            if (is !=null){
                try {
                    is.close();
                } catch (IOException e) {
                    logger.log(Level.WARNING,"发送失败");
                }
            }
        }
    }
 }
