package com.inspur.mapreduce.ignore;

import com.inspur.mapreduce.championcnt.ChampionCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.File;
import java.io.IOException;

/**
 * 任务4：在进入map之前就过滤掉NBA分区之前也就是1970年之前的数据
 * 所以用自定义输入
 */
public class SkipFormeerly {

    //自定义MyInputFormat
    static class MyInputFormat extends FileInputFormat<LongWritable,Text>{

        //创建MyRecordReader对象
        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new MyRecordReader();
        }
    }

    /**
     * 自定义RecordReader类
     */
    static class MyRecordReader extends RecordReader<LongWritable,Text>{
        //读取开始位置
        private long start;

        //当前位置
        private long pos;

        //读取结束位置
        private long end;

        //行读取器
        private LineReader in;

        //流对象，用于创建LineReader对象
        private FSDataInputStream fileIn;

        //用于保存key的序列化对象
        private LongWritable key=new LongWritable();

        //用于保存value的序列化对象
        private Text value=new Text();

        //完成初始化，只是初始化一次。
        //参数：split：读取的切片对象
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            //因读取的文件比较大，需强制转化为FileSplit
            FileSplit fsplit=(FileSplit) split;
            start=fsplit.getStart();//设置开始位置
            end=start+fsplit.getLength();//设置结束位置
            //通过FileSystem对象实例化FSDataInputStream
            Configuration conf=context.getConfiguration();
            Path path=fsplit.getPath();
            FileSystem fs=path.getFileSystem(conf);
            fileIn=fs.open(path);
            //通过FSDataInputStream创建LineReader对象
            in=new LineReader(fileIn);
            //将读取点移动到开始位置
            fileIn.seek(start);
            if(start!=0){
                start+=in.readLine(new Text(),0,(int)Math.min(Integer.MAX_VALUE,end-start));
            }
            pos=start;//将当前位置设为开始位置
        }

        //为key和value赋值
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            //判断当前位置，如果读取位置超过结束位置则终止读取
            if(pos>end){
                return false;
            }

            //循环读取数据，解析数据，如果是1970之前的数据则不保存到key和value
            while(true){
                pos+=in.readLine(value);  //value:本行的文本数据
                if(value.getLength()==0){  //如果本次读取数据长度为0则终止读取
                    return false;
                }
                //处理数据:如果是1970之前的数据则不保存到key和value
                String[] fields=value.toString().split(",");
                int year=Integer.parseInt(fields[0]);
                if(year<1970){
                    continue;
                }
                key.set(pos);//key:记录偏移量
                return true;
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            //因为在nextKeyValue（）对key进行了赋值，所以直接返回
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            //因为在nextKeyValue（）对value进行了赋值，所以直接返回
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        //关闭流类资源
        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    //Mapper
    static class MyMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

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
    static class MyReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
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
    static class MyPartitioner extends Partitioner<Text,IntWritable> {

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

        job.setJarByClass(SkipFormeerly.class); //指定job
        job.setMapperClass(MyMapper.class);//指定mapper
        job.setMapOutputKeyClass(Text.class);  //指定map执行后输出的key类型
        job.setMapOutputValueClass(IntWritable.class);//指定map执行后输出的value类型

        job.setReducerClass(MyReducer.class);//指定Reducer类
        job.setOutputKeyClass(Text.class);//指定reducer执行后输出的key类型
        job.setOutputValueClass(IntWritable.class);//指定reducer执行后输出的value类型
        job.setCombinerClass(MyReducer.class);

        job.setPartitionerClass(MyPartitioner.class);//指定自定义分区的文件
        job.setNumReduceTasks(3);//指定reduce的数量为3，个数有分区决定，咱是0-1-2共3个分区

        job.setInputFormatClass(MyInputFormat.class);//引入自定义的输入类

        FileInputFormat.setInputPaths(job,new Path("file/out/dataclear/out/part-m-00000"));//指定要分析的文件
        FileOutputFormat.setOutputPath(job,new Path("file/out/SkipFormerly/out"));
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);//提交任务
    }
}