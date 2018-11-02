package mr;

import IpAnalysis.IpUtil;
import IpAnalysis.userAgentUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogAnalysis {
    static class MyMapper extends Mapper<LongWritable, Text,NullWritable,Text>{
        Text v = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringBuffer kvsb = new StringBuffer();
            String string = value.toString();
            //切割
            String[] split = string.split("\\^A");
            //获取ip地址
            String ip = split[0];
            //获取时间戳
            String time = split[1];
            //获取剩余的字段
            String[] keyvalues = split[3].split("\\?")[1].split("&");
            kvsb.append("IP:"+split[0]+"\t");
            kvsb.append("Address:"+ IpUtil.getRegionInfoByIp(split[0])+"\t");
            kvsb.append("TimeStamp:"+split[1]+"\t");
            for (String kv : keyvalues) {
                String[] strings = kv.split("=");
                String k = strings[0];
                String v = strings[1];
                if (k.equals("en")) {
                    kvsb.append("Event:"+v+"\t");
                } else if (k.equals("ver")) {
                    kvsb.append("Version:"+v+"\t");
                } else if (k.equals("pl")) {
                    kvsb.append("Platform:"+v+"\t");
                } else if (k.equals("sdk")) {
                    kvsb.append("Sdk:"+v+"\t");
                } else if (k.equals("u_ud")) {
                    kvsb.append("Uuid:"+v+"\t");
                } else if (k.equals("l")) {
                    kvsb.append("Language:"+v+"\t");
                } else if (k.equals("b_iev")) {
                    kvsb.append("useAgent"+userAgentUtil.parserUserAgent(v)+"\t");
                } else if (k.equals("b_rst")) {
                    kvsb.append("Resolution:"+v+"\t");
                }
            }
            String[] split1 = kvsb.substring(0, kvsb.length() - 1).split("\t");
//            for (String s : split1) {
//               v.set(s);
//            }
           // context.write(NullWritable.get(),v);
            context.write(NullWritable.get(),new Text(kvsb.toString().substring(0,kvsb.length()-1)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
//    static class MyReducer extends Reducer<Text,Text,Text,NullWritable>{
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            super.setup(context);
//        }
//
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            context.write(key,NullWritable.get());
//        }
//
//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//            super.cleanup(context);
//        }
//    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取配置对象
        Configuration conf = new Configuration();
        //获取hdfs连接参数
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        //conf.set("mapreduce.framework.name","local");
        //创建一个对象
        Job job = Job.getInstance(conf,"LogAnalysis");
        //设置job的执行路径
        job.setJarByClass(LogAnalysis.class);
        //设置Mapper的执行的业务类
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
//        //设置Reduce的相关参数
//        job.setReducerClass(MyReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);
        //设置job的输入文件目录
        FileInputFormat.setInputPaths(job,new Path("/logs"));
        //设置文件的输出文件目录
        FileOutputFormat.setOutputPath(job,new Path("/out"));

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
