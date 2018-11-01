package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogAnalysis {
    static class MyMapper extends Mapper<LongWritable, Text,Text,NullWritable>{
        Text k = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            //切割
            String[] split = string.split("^A");
            //获取ip地址
            String ip = split[0];
            //获取时间戳
            String time = split[1];
            k.set(ip);
            k.set(time);
            context.write(k,NullWritable.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
    static class MyReducer extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取配置对象
        Configuration conf = new Configuration();
        //获取hdfs连接参数
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        conf.set("mapreduce.framework.name","local");
        //创建一个对象
        Job job = Job.getInstance(conf,"LogAnalysis");
        //设置job的执行路径
        job.setJarByClass(LogAnalysis.class);
        //设置Mapper的执行的业务类
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
//        //设置Reduce的相关参数
//        job.setReducerClass(MyReduce.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(DoubleWritable.class);
        //设置job的输入文件目录
        FileInputFormat.setInputPaths(job,new Path("hdfs://hadoop01:9000/logs"));
        //设置文件的输出文件目录
        FileOutputFormat.setOutputPath(job,new Path("F:\\千峰学习\\Hadoop学习资料\\Hadoop第六天\\day11\\GP1809\\DataOut"));

        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
