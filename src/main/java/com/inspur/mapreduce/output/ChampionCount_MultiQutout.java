package com.inspur.mapreduce.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
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
public class ChampionCount_MultiQutout {

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
    static class MyOutPutFormat extends FileOutputFormat <Text,IntWritable>{


        @Override
        public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new MyRecordWriter();
        }
    }
    static class MyRecordWriter extends RecordWriter<Text,IntWritable> {
        //创建三个fsdateoutputstream对象，实现三类数据的输出
        private FSDataOutputStream areaEast;
        private FSDataOutputStream areaWest;
        private FSDataOutputStream noArea;

        public MyRecordWriter() throws IOException{
            Configuration conf=new Configuration();
            FileSystem fs=FileSystem.get(conf);
            areaEast=fs.create(new Path("file/out/ChampionCount_MultiQutout/out/areaEast.txt"));
            areaWest=fs.create(new Path("file/out/ChampionCount_MultiQutout/out/areaWest.txt"));
            noArea=fs.create(new Path("file/out/ChampionCount_MultiQutout/out/noArea.txt"));
        }

        //将统计结果写入
        @Override//key：芝加哥公牛队E value:6
        public void write(Text key, IntWritable value) throws IOException, InterruptedException {
            String split[]=key.toString().split(",");

            if(split.length>1){
                String area=split[1];//得到分区标志
                if("E".equals(area)){
                    areaEast.write((key.toString()+"  "+value.toString()).getBytes());
                    areaEast.write("\n ".getBytes());

                }else if("W".equals(area)){
                    areaWest.write((key.toString()+"  "+value.toString()).getBytes());
                    areaWest.write("\n ".getBytes());
                }
            }else{
                noArea.write((key.toString()+"  "+value.toString()).getBytes());
                noArea.write("\n ".getBytes());
            }
        }

        //关闭流资源
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            IOUtils.closeStream(areaEast);
            IOUtils.closeStream(areaWest);
            IOUtils.closeStream(noArea);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);  //建立任务

        job.setJarByClass(ChampionCount_MultiQutout.class); //指定job
        job.setMapperClass(MyMapper.class);//指定mapper
        job.setMapOutputKeyClass(Text.class);  //指定map执行后输出的key类型
        job.setMapOutputValueClass(IntWritable.class);//指定map执行后输出的value类型

        job.setReducerClass(MyReducer.class);//指定Reducer类
        job.setOutputKeyClass(Text.class);//指定reducer执行后输出的key类型
        job.setOutputValueClass(IntWritable.class);//指定reducer执行后输出的value类型
        job.setCombinerClass(MyReducer.class);


        //使用自定义的输出
        job.setOutputFormatClass(MyOutPutFormat.class);

        FileInputFormat.setInputPaths(job,new Path("file/out/dataclear/out/part-m-00000"));//指定要分析的文件
        FileOutputFormat.setOutputPath(job,new Path("file/out/ChampionCount_MultiQutout/out"));
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);//提交任务
    }
}
