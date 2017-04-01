/**
 * Created by mxins@qq.com on 2017/3/21.
 * usage: spark-submit --class com.followeye.test.JavaWordCount java/out/artifacts/test_jar/test.jar
 */
package com.followeye.test;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {
    private static final Pattern comma = Pattern.compile(",");

    public static void main(String[] args) throws Exception {
        long t1 = System.currentTimeMillis();
        String basedir = System.getProperty("user.dir");
        String path = basedir + "/data/consume_log/";
        String output = basedir + "/output/java_wordcount/";
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(path).javaRDD();

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(comma.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });
        long t2 = System.currentTimeMillis();
        counts.saveAsTextFile(output);
        long t3 = System.currentTimeMillis();
        System.out.println("java wordcount, cost:" + (t2 - t1) + "," + (t3 - t2));
//        List<Tuple2<String, Integer>> output = counts.collect();
//        for (Tuple2<?,?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }
        spark.stop();
    }
}
