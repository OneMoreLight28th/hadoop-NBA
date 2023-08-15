package com.inspur.mapreduce.dataclear;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务1：数据清洗
 * （1）球队新旧名字的替换：NBA的历史较为久远，从1947年至2019年的这段时间里，一些球队已经不存在了（例如：芝加哥牡鹿队），还有部分球队的队名发生了变化（例如：明尼阿波利斯湖人队，现在的名称是洛杉矶湖人队）；所以，对于已经不存在的球队，继续保存其名称，不做修改；但是已经更改名称的球队，需要映射为现在球队的名称；
 * （2）球队添加上东西分区的标志。另外，因为要对球队进行东西分区的统计，所以要对球队添加东西分区的标识
 */
public class DataClear {

    public static class MyMapper extends Mapper<LongWritable, Text, NullWritable,Text>{
        //两个java.util.Map对象，用于封装新老球队和东西分区球队的映射
        Map<String,String> nameMap=new HashMap<String,String>();//封装新老球队的映射
        Map<String, List<String>> areaMap=new HashMap<String, List<String>>();//东西分区球队的映射
        Text newValue=new Text();//记录map输出的value值的
        /**
         * 在map方法之前执行，一般做初始化工作，也就是预备工作，只执行一次
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            //新旧球队名单，key:旧的球队名 value:新球队名
            nameMap.put("费城勇士队","金州勇士队");
            nameMap.put("旧金山勇士队","金州勇士队");
            nameMap.put("明尼阿波利斯湖人队","洛杉矶湖人队");
            nameMap.put("塞拉库斯民族队","费城76人队");
            nameMap.put("罗切斯特皇家队","萨克拉门托国王队");
            nameMap.put("圣路易斯老鹰队","亚            特兰大老鹰队");
            nameMap.put("华盛顿子弹队","华盛顿奇才队");
            nameMap.put("巴尔的摩子弹队","华盛顿子弹队");
            nameMap.put("西雅图超音速队","俄克拉荷马城雷霆队");
            nameMap.put("福特维恩活塞队","底特律活塞队");
            nameMap.put("新泽西网队","布鲁克林篮网队");
            nameMap.put("达拉斯小牛队","达拉斯独行侠队");
            //东城区的球队名单
            List<String> eastList=new ArrayList<String>();
            eastList.add("亚特兰大老鹰队");
            eastList.add("夏洛特黄蜂队");
            eastList.add("迈阿密热火队");
            eastList.add("奥兰多魔术队");
            eastList.add("华盛顿奇才队");
            eastList.add("波士顿凯尔特人队");
            eastList.add("布鲁克林篮网队");
            eastList.add("纽约尼克斯队");
            eastList.add("费城76人队");
            eastList.add("多伦多猛龙队");
            eastList.add("芝加哥公牛队");
            eastList.add("克里夫兰骑士队");
            eastList.add("底特律活塞队");
            eastList.add("印第安纳步行者队");
            eastList.add("密尔沃基雄鹿队");
            //西城区的球队名单
            List<String> westList=new ArrayList<String>();
            westList.add("达拉斯独行侠队");
            westList.add("休斯顿火箭队");
            westList.add("孟菲斯灰熊队");
            westList.add("圣安东尼奥马刺队");
            westList.add("丹佛掘金队");
            westList.add("明尼苏拉森林狼队");
            westList.add("俄克拉荷马城雷霆队");
            westList.add("波特兰开拓者队");
            westList.add("犹他爵士队");
            westList.add("金州勇士队");
            westList.add("洛杉矶湖人队");
            westList.add("菲尼克斯太阳队");

            areaMap.put("east",eastList);
            areaMap.put("west",westList);
        }
        //数据清洗：key:偏移量 value:nba.csv读出一每行记录，一行记录执行一次map方法
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            //使用,拆分nba.csv文件中的每行记录
            String[] split=value.toString().split(",");
            //(1)新旧球队名替换
            //获取冠军队名
            String champoin=split[2];
            //获取亚军队名
            String second=split[4];
            //进行新旧替换
            String newName=nameMap.get(champoin);
            champoin=(newName==null?champoin:newName);//冠军队名替换
            String newName2=nameMap.get(second);
            second=(newName2==null?second:newName2);//亚军队名替换
            split[2]=champoin;
            split[4]=second;

            //（2）1970年以后的球队，添加东西区标识
            int year=Integer.parseInt(split[0]);

            if(year>=1970){
                List<String> eastList=areaMap.get("east");
                List<String> westList=areaMap.get("west");
                //给冠军队添加东西区的标志
                String areaFlag="";
                if(eastList.contains(champoin)){
                    areaFlag="E";
                }else if(westList.contains(champoin)){
                    areaFlag="W";
                }
                newValue.set(strArrToString(split)+","+areaFlag); //将字符串转换为Text类型
                //如：1981,5.5-5.14,波士顿凯尔特人队,4-2,休斯顿火箭队,塞德里克·麦克斯维尔,W
            }else{
                newValue.set(strArrToString(split));
            }

            context.write(NullWritable.get(),newValue);

        }

        //字符串拼接：样例：1981,5.5-5.14,波士顿凯尔特人队,4-2,休斯顿火箭队,塞德里克·麦克斯维尔
        private String strArrToString(String[] split){
            StringBuilder builder=new StringBuilder();
            for(int i=0;i<split.length;i++){
                builder.append(split[i]);
                if(i!=split.length-1){
                    builder.append(",");
                }
            }
            return builder.toString();
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);

        job.setJarByClass(DataClear.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //不用设reduce，则reduce不需要启动进程运行reduce，reduce任务数量设为0,节省资源，如果不设置，默认是mapreduce会自动提供一个
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path("file/dataclear/nba.csv"));
        FileOutputFormat.setOutputPath(job,new Path("file/out/dataclear/out"));
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交任务
        job.waitForCompletion(true);
    }

}
