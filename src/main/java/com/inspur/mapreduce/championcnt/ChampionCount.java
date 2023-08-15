package com.inspur.mapreduce.championcnt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 任务2：
 *  分析的源文件：file/out/dataclear/out/part-m-00000
 * （1）统计冠军队获得冠军的次数。
 * （2）并按照E W 无 进行分区存放
 *
 */
public class ChampionCount {

    //Mapper
    static class MyMapper extends Mapper<LongWritable, Text,Text,IntWritable> {

        Text newKey=new Text();
        IntWritable newValue=new IntWritable(1);

        //输入进map的key和value：LongWritable key：行的偏移量, Text value：读入的行的值
        //从map中输出的key和value：key,如洛杉矶湖人队,W   value:1
        @Override
        protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

            String split[]=value.toString().split(",");

            System.out.print(split);
            String champion=split[2];//得到冠军队名字
            String area=null;
            if(split.length==7){
                area=split[6];//得到分区标志
            }
            if(area!=null){
                newKey.set(champion+","+area); //如果有分区标志则：key样例数据如洛杉矶湖人队,W
            }else{
                newKey.set(champion);//如果没有分区标志，则key为冠军队的名字
            }
            context.write(newKey,newValue);
        }
    }

    //Reducer:统计
    static class MyReducer extends Reducer<Text, IntWritable,Text,IntWritable>{
        @Override
        //输入进入reduce的类型：key：洛杉矶湖人队,W  ,values[1,1,1,1]
        //从reduce输出的类型：key：洛杉矶湖人队,W value：4
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable iw:values){
                sum+=iw.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    //进行分区：规则： 并按照E W 无 进行分区存放
    static class MyPartitioner extends Partitioner<Text,IntWritable>{

        @Override
        public int getPartition(Text key, IntWritable value, int i) {
            String split[]=key.toString().split(",");
            String area=null;//无分区标志
            if(split.length>1){  //key :冠军队名,W
                area=split[1];//得到分区标志E/W
            }

            //根据area变量的取值，放入不同的分区
            if("E".equals(area)){
                return 0;//如果area=‘E’,放到编号为0的分区中
            }else if("W".equals(area)){
                return 1;//如果area=‘W’,放到编号为1的分区中
            }else{
                return 2;//如果无area，则放到编号为2的分区中
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);  //建立任务

        job.setJarByClass(ChampionCount.class); //指定job
        job.setMapperClass(MyMapper.class);//指定mapper
        job.setMapOutputKeyClass(Text.class);  //指定map执行后输出的key类型
        job.setMapOutputValueClass(IntWritable.class);//指定map执行后输出的value类型

        job.setReducerClass(MyReducer.class);//指定Reducer类
        job.setOutputKeyClass(Text.class);//指定reducer执行后输出的key类型
        job.setOutputValueClass(IntWritable.class);//指定reducer执行后输出的value类型
        job.setCombinerClass(MyReducer.class);

        job.setPartitionerClass(MyPartitioner.class);//指定自定义分区的文件
        job.setNumReduceTasks(3);//指定reduce的数量为3，个数有分区决定，咱是0-1-2共3个分区

        FileInputFormat.setInputPaths(job,new Path("file/out/dataclear/out/part-m-00000"));//指定要分析的文件
        FileOutputFormat.setOutputPath(job,new Path("file/out/championCount/out"));
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);//提交任务
    }
}
