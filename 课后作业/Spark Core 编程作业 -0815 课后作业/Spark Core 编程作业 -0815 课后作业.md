## 作业一 使用RDD API实现带词频的倒排索引

### 题目
> 倒排索引（Inverted index），也被称为反向索引。它是文档检索系统中最常用的数据结构。被广泛 地应用于全文搜索引擎。

例子如下，被索引的文件为（0，1，2代表文件名）
```shell
0. "it is what it is"
1. "what is it"
2. "it is a banana"
```
我们就能得到下面的反向文件索引：
``` json
"a": {2} 
"banana": {2} 
"is": {0, 1, 2} 
"it": {0, 1, 2} 
"what": {0, 1} 
```
再加上词频为： 
``` json
"a": {(2,1)} 
"banana": {(2,1)} 
"is": {(0,2), (1,1), (2,1)} 
"it": {(0,2), (1,1), (2,1)} 
"what": {(0,1), (1,1)}
```

### 让我想想
- wholeTextFiles 可以实现遍历目录，并且获取文件名

### 代码
```java
public class InvertedIndex {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName(InvertedIndex.class.getSimpleName());

        JavaSparkContext sc = new JavaSparkContext(conf);

        //读取整个目录下的文件
        JavaPairRDD<String, String> files = sc.wholeTextFiles(args[0]);

        //拆分单词，并让单词携带文件名信息
        JavaRDD<Tuple2<String, String>> word = files.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, String> fileNameContent) throws Exception {
                String[] filePath = fileNameContent._1.split("/");

                String fileName = filePath[filePath.length - 1];
                String line = fileNameContent._2;
                ArrayList<Tuple2<String, String>> result = new ArrayList<>();
                for (String word : line.split(" ")) {
                    result.add(new Tuple2<>(word, fileName));
                }
                return result.iterator();
            }
        });

        // 分组聚合
        JavaPairRDD<String, String> rdd = word.mapToPair((PairFunction<Tuple2<String, String>, String, String>) t -> new Tuple2<>(t._1, t._2));
        JavaPairRDD<String, String> rdd1 = rdd.reduceByKey((Function2<String, String, String>) (v1, v2) -> v1 + "|" + v2)
                .sortByKey();

        // 汇总统计
        JavaRDD<Tuple2<String, Map<String, Integer>>> rdd2 = rdd1.map(t -> {
            Map<String, Integer> map = new HashMap<>();
            for (String s : t._2.split("\\|")) {
                map.put(s, map.getOrDefault(s, 0) + 1);
            }
            return new Tuple2<>(t._1, map);
        });

        //打印
        for (Tuple2<String, Map<String, Integer>> next : rdd2.collect()) {
            List<String> list = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : next._2.entrySet()) {
                list.add("(" + entry.getKey() + ", " + entry.getValue() + ")");
            }
            System.out.println("\"" + next._1 + "\": {" + StringUtils.join(",", list) + "}");
        }
        sc.close();
    }
}
```

### 运行结果
![运行结果](../resource/spark01.png)

## 作业二 Distcp的spark实现

### 题目
使用Spark实现Hadoop 分布式数据传输工具 DistCp (distributed copy)，只要求实现最基础的copy功能，对于-update、-diff、-p不做要求 

对于HadoopDistCp的功能与实现，可以参考 
- https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html 
- https://github.com/apache/hadoop/tree/release-2.7.1/hadoop-tools/hadoop-distcp

> Hadoop使用MapReduce框架来实现分布式copy，在Spark中应使用RDD来实现分布式copy

- 应实现的功能为： sparkDistCp hdfs://xxx/source hdfs://xxx/target 
- 得到的结果为：启动多个task/executor，将hdfs://xxx/source目录复制到hdfs://xxx/target，得到 hdfs://xxx/target/source 
  - 需要支持source下存在多级子目录 
  - 需支持-i Ignore failures 参数 
  - 需支持-m max concurrence参数，控制同时copy的最大并发task数

### 让我想想
1. RDD 为文件列表
2. mapPartitions 实现分布式
### 代码
```java
public class Distcp {
    public static void main(String[] args) throws IOException, ParseException {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName(Distcp.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        String s_path = args[args.length - 2];
        String t_path = args[args.length - 1];

        //获取配置参数
        int m = 3;
        boolean ignoreError = false;

        CommandLine cmdLine = getOptions(args);
        if (cmdLine.hasOption("m")) {
            m = Integer.parseInt(cmdLine.getOptionValue("m"));
        }

        if (cmdLine.hasOption("i")) {
            ignoreError = true;
        }

        Configuration configuration = sc.hadoopConfiguration();
        //还原输出路径
        FileSystem fs = FileSystem.get(configuration);

        Path srcPath = new Path(s_path);
        Path tarPath = new Path(t_path);
        if (fs.isDirectory(tarPath) && fs.listStatus(tarPath).length > 0 && !ignoreError) {
            System.err.println("the target dir not empty");
            System.exit(1);
        }

        //文件对应关系：files 中的 Tuple2 _1 是原文件路径 _2 是目标文件路径
        List<Tuple2<String, String>> files = new ArrayList<>();
        if (fs.isDirectory(srcPath)) {
            //检查文件夹是否存在
            checkDirectories(s_path, fs, s_path, t_path);
            //生成文件对应关系
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(srcPath, true);
            while (listFiles.hasNext()) {
                LocatedFileStatus next = listFiles.next();
                Path path = next.getPath();
                String fileSrcPathStr = next.getPath().toUri().getPath();
                String fileTarPathStr = t_path + fileSrcPathStr.split(s_path)[1];
                files.add(new Tuple2<>(path.toUri().getPath(), fileTarPathStr));
            }

        } else if (fs.isFile(srcPath)) {
            files.add(new Tuple2<>(s_path, t_path));
        } else {
            System.out.println(s_path + " is not a directory or file");
            System.exit(1);
        }

        if (!files.isEmpty()) {
            Broadcast<SerializableWritable<Configuration>> broadcast = sc.broadcast(new SerializableWritable<>(sc.hadoopConfiguration()));
            distCopy(sc, files, m, broadcast);
        }

        sc.close();

    }

    /**
     * 获取参数
     */
    private static CommandLine getOptions(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("m", true, "max concurrence");
        options.addOption("i", false, "Ignore failures");

        CommandLineParser parser = new BasicParser();

        return parser.parse(options, args);
    }

    /**
     * 递归检查目标目录是否存在
     */
    private static void checkDirectories(String srcPathStr, FileSystem fs, String inputPathStr, String outPathStr) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(srcPathStr));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                String dirName = fileStatus.getPath().toString().split(inputPathStr)[1];
                Path newTarPath = new Path(outPathStr + dirName);
                if (!fs.exists(newTarPath)) {

                    fs.mkdirs(newTarPath);
                }
                checkDirectories(inputPathStr + dirName, fs, inputPathStr, outPathStr);
            }
        }
    }


    /**
     * 启动 spark 开始拷贝文件
     */
    private static void distCopy(JavaSparkContext sc, List<Tuple2<String, String>> files, int m, Broadcast<SerializableWritable<Configuration>> broadcast) {
        JavaRDD<String> copyResultRDD = sc.parallelize(files, m).mapPartitions(t -> {
            List<String> result = new ArrayList<>();
            Configuration configuration = broadcast.getValue().value();
            FileSystem fs = FileSystem.get(configuration);

            while (t.hasNext()) {
                Tuple2<String, String> next = t.next();
                Path src = new Path(next._1);
                Path dst = new Path(next._2);
                FileUtil.copy(fs, src, fs, dst, false, configuration);
                result.add("copy " + next._1 + " to " + next._2 + " success by worker #");
            }
            return result.iterator();
        });

        List<String> collect = copyResultRDD.collect();

        for (String s : collect) {
            System.out.println(s);
        }
    }
}
```

### 运行结果
![运行结果](../resource/spark02.png)