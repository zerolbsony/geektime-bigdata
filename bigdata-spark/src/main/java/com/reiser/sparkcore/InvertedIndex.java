package com.reiser.sparkcore;

import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * @author: reiserx
 * Date:2021/8/17
 * Des: 使用 spark 实现倒排索引
 */
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
