
### **任务1：需求描述**
案例中需要处理的文件为nba.csv，该文件记录了NBA历年总冠军的详细情况，文件的字段从左到右依次为比赛年份、具体日期、冠军、比分、亚军和当年MVP（联盟MVP是Most Valuable Player缩写，即最有价值球员），每个字段以半角逗号“,”进行分割，如图3-1所示。

![nba\_csv](/README/001.png "nba\_csv")

图3-1NBA原始文件数据

本课程设计要求对此数据集做如下处理：

（1）数据清洗；

（2）统计各球队获得冠军数量；并将东西部球队的统计结果分别存储。
### **任务1：实现过程**
`  `**（1）要求**

NBA的历史较为久远，从1947年至2019年的这段时间里，一些球队已经不存在了（例如：芝加哥牡鹿队），还有部分球队的队名发生了变化（例如：明尼阿波利斯湖人队，现在的名称是洛杉矶湖人队）；所以，对于已经不存在的球队，继续保存其名称，不做修改；但是已经更改名称的球队，需要映射为现在球队的名称；

另外，因为要对球队进行东西分区的统计，所以要对球队添加东西分区的标识。

**（2）解题思路**

添加球队新老名称的映射，读取每行数据时，遇到老的名称，将其替换为新名称；

添加东西分区球队的映射，读取数据时，分析冠军球队所在分区，然后添加标识（东部球队以“E”标识，西部球队以“W”标识）

需要注意的是，美国NBA联盟是从1970年开始进行东西分区的，因此需要对年份进行判断。

**（3）核心代码解析**

在自定义的Mapper类中，我们先创建了两个java.util.Map对象，用于封装新老球队和东西分区球队的映射，核心代码如图所示：

![](/README/002.png)

图3-2封装新老球队和东西分区的映射对象

映射数据的初始化最好是放在Mapper类的setup()方法中，Mapper类有四个方法，

protected void setup(Context context)

Protected void map(KEYIN key,VALUEIN value,Context context)

protected void cleanup(Context context)

public void run(Context context)

setup()方法一般用来加载一些初始化的工作,像关联数据的初始化、建立数据库的链接等等；cleanup()方法是收尾工作,如关闭文件或者执行map()后的键值分发等；map()函数则是描述对每行数据的处理逻辑；run()方法定义了以上几个方法的执行过程，如图3-3所示，通过此方法，我们可以看出，setup()与cleanup()在Mapper对象的生命周期中只被调用一次，而map()方法则是只要有新的key和value，就会被调用。

![](/README/003.png)

图3-3 Mapper类中的run()方法

setup()方法中映射数据的初始化的核心代码如图3-4所示。

![](/README/004.png)

图3-4 映射数据初始化核心代码

东西分区的List列表最后要添加到areaMap对象中，如图3-5所示。

![](/README/005.png)

图3-5 分区添加到areaMap对象

在map方法中，按照上面的要求进行逻辑映射即可，map最终输出的结果是<NullWritable,Text>类型，Text部分是替换了新名称，且附加了东西区标识的数据，核心代码如图3-6所示。

![](/README/006.png)

图3-6 map方法核心代码

main方法中需要定义job启动的相关参数，数据清洗部分实际只需要将map阶段的结果进行输出，而不需要reduce部分的汇总处理，因此不需要配置Reducer，但即使不编写Reducer类，MapReduce框架也会添加一个默认的Reducer类，即org.apache.hadoop.mapreduce.Reducer。可以通过Job对象的setNumReduceTasks方法，并设置其参数为0，来避免多余的性能浪费，核心代码如图3-7所示。

![](/README/007.png)

图3-7main方法核心代码
## **任务2：统计各球队获得冠军数量，统计结果按照东区、西区和未分区三个文件存储。**
### **任务2：解题思路** 
要统计各球队获得冠军的数量，基本思路与前面wordcount程序的逻辑是一致的，在map阶段解析出冠军球队的名称作为键，以一个值为1的IntWritable对象作为值，然后传递给reduce，在reduce部分做相加操作即可。

另外统计结果需要根据东西区来分文件存储，因此相对wordcount要稍微复杂。通过已给出并清洗后的数据集可知，应该分为东区、西区和未分区三个文件存储，因此需要自定义Partitoner，并根据分区标识来判断每行数据需要进入哪个分区。

指定reduce的个数，通过job.setNumReduceTasks(3)来设置reduce的数量为3个。
### **任务2：核心部分代码解析**
map部分主要用于解析冠军球队名称，以及解析出分区标识，并根据分区标识来设置键值，而值部分则直接取值为1，核心代码如图3-9所示。

![](/README/008.png)

图3-9 map核心代码

在自定义的Partitioner中，需要根据键的取值，来判断每行数据进入哪一个分区，核心代码如图3-10所示。

![](/README/009.png)

图3-10 Partitioner核心代码

reduce部分进行合并，统计每只球队获得冠军的数量，核心代码如图3-11所示。

![](/README/010.png)

图3-11reduce核心代码

main方法中，除了基本的job提交所需的参数外，此处还指定了Combiner，由于计算数量的逻辑与reduce一致，因此直接使用了MyReducer类。另外还需要指定自定义分区类MyPartitioner类，并且还设置了reduce的数量为3，核心代码如图3-12所示。

![](/README/011.png)

图3-12 main方法核心代码

最终的计算结果有三个文件（除\_SUCCESS文件），文件列表如图3-13所示。

|![](/README/012.png)|
| - |
` `图3-13 结果文件列表

三个文件的内容如下所示：

（1）part-r-00000内容如图3-14所示。

|![](/README/013.png)|
| - |
图3-14 part-r-00000内容

（2）part-r-00001内容如图3-15所示。

|![](/README/014.png)|
| - |
图3-15 part-r-00001内容

（3）part-r-00002内容如图3-16所示。

|![](/README/015.png)|
| - |
图3-16 part-r-00002内容


### **任务3：统计各球队获得冠军数量，统计结果使用自定义OutputFormat来实现多文件存储**
下面我们会对上面的案例进行修改，通过自定义OutputFormat的方式将东区、西区和没有分区的数据分别保存在三个文件中。

（1）解题思路

OutputFormat是MapReduce框架用于数据输出的抽象父类，FileOutputFormat类继承了OutputFormat，用于定义文件的输出，在reduce阶段，默认的输出类是继承了FileOutputFormat的TextOutputFormat，我们可以通过继承FileOutputFormat来实现自己的输出逻辑。

（2）核心部分代码解析

自定义的MyOutputFormat非常简单，只是在其getRecordWriter方法中返回一个RecordWriter对象即可，核心代码如图3-17所示。

![](/README/016.png)

图3-17 MyOutputFormat核心代码

我们需要继承RecordWriter来实现自己的输出逻辑，本案例定义了一个MyRecordWriter类，在其构造方法中，创建了三个FSDataOutputStream对象，分别用于不同类型数据的输出。在其核心的write方法中，根据分区表示来指定不同分区数据的输出，并在最后的close方法中使用hadoop的IOUtils来关闭三个FSDataOutputStream，核心代码如图3-18所示。

![](/README/017.png)

图3-18 MyRecordWriter核心代码

需要注意的是，在main方法中，由于使用了自定义的OutputFormat，因此不再需要自定义Partitioner，另外还需要指定OutputFormat的class为MyOutputFormat，核心代码如图3-19所示。

![](/README/018.png)

图3-19 main方法核心代码

最终的输出结果与上一个示例基本一致，只是文件名称是本例代码中直接定义的，最终生成的结果文件列表如图3-20所示。

|![](/README/019.png)|
| :-: |
图3-20 结果文件列表
###
### **任务4：只处理东西分区后的数据**
（1） 解题思路

在前面两个示例中，都实现了数据的多文件存储，但由于本案例的要求是只将东西分区的数据分文件存储，因此单独保存1970年之前的数据有些多余。去掉这些多余的数据非常简单，只需要在示例2和3的map方法中进行过滤即可，但更好的方式是自定义InputFormat类，实现在数据进入map方法之前就将其去掉。

InputFormat是MapReduce框架进行数据读取的抽象父类，FileInputFormat继承了InputFormat，主要用于文件的读取。框架默认使用的是继承了FileInputFormat的TextInputFormat，我们可以继承FileInputFormat来实现自己的输出逻辑。

（2）核心部分代码解析

创建MyInputFormat类继承FileInputFormat，并实现createRecordReader方法，此方法中返回一个自定义的MyRecordReader对象，核心代码如图3-21所示。

![](/README/020.png)

图3-21 MyInputFormat核心代码

实现自定义的RecordReader需要重写如下几个方法：

`     `public void initialize(InputSplit split, TaskAttemptContext context)

public boolean nextKeyValue()

public LongWritable getCurrentKey()

public Text getCurrentValue()

public float getProgress()

public void close()

其中，initialize方法主要用于初始化一些数据读取需要的参数，如开始位置、结束位置、读取流对象等；nextKeyValue方法将在数据读入map方法之前被调用（参见图3-3），其作用是给键和值赋值；getCurrentKey和getCurrentValue方法用于获取已赋值的键和值，并传入map方法中；getProgress用于计算RecordReader读取了多少数据；close用来关闭读取流。

在MyRecordReader中定义用于读取操作的变量，核心代码如图3-22所示。

![](/README/021.png)

图3-22 定义读写操作变量

在initialize方法中，对以上变量进行初始化，核心代码如图3-23所示。

![](/README/022.png)

图3-23 initialize方法核心代码

在nextKeyValue方法中，进行过滤，1970年之前的数据将不会被传入到map方法中，核心代码如图3-24所示。

![](/README/023.png)

图3-24 nextKeyValue方法核心代码

实现getCurrentKey和getCurrentValue方法,核心代码如图3-25所示。

![](/README/024.png)

图3-25 getCurrentKey和getCurrentValue方法核心代码

将以上代码添加到示例2或3中，然后在main方法中设置InputFormat的class即可，核心代码如图3-26所示。

![](/README/025.png)

图3-26 main方法核心代码

运行后的最终输出结果中,不会存在分区之前的数据了。
