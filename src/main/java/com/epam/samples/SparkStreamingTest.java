package com.epam.samples;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.regex.Pattern;

public class SparkStreamingTest implements Serializable{
    private static final Pattern SPACE = Pattern.compile(" ");

    public SparkConf getSparkConf(String appName, String master) {
        return new SparkConf().setAppName(appName).setMaster(master);
    }

    public JavaStreamingContext getSparkContext(SparkConf sparkConf, int duration) {
        return new JavaStreamingContext(sparkConf, Durations.seconds(duration));
    }

    public void process(JavaStreamingContext jssc, String hostName, int port) {
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(hostName, port, StorageLevels.MEMORY_AND_DISK_SER);
        JavaDStream<String> words = convertLineToWords(lines);
        JavaPairDStream<String, Integer> pairs = emitKeys(words);
        JavaPairDStream<String, Integer> wordCount = countWords(pairs);
        wordCount.print();
        jssc.start();
        jssc.awaitTermination();
    }

    private JavaDStream<String> convertLineToWords(JavaReceiverInputDStream<String> lines) {
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
                return Lists.newArrayList(SPACE.split(s));
            }
        });
        return words;
    }

    private JavaPairDStream<String, Integer> emitKeys(JavaDStream<String> words) {
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });
        return pairs;
    }

    private JavaPairDStream<String, Integer> countWords(JavaPairDStream<String, Integer> pairs) {
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });
        return wordCounts;
    }


    public static void main(String[] args) {
        SparkStreamingTest test = new SparkStreamingTest();
        SparkConf conf = test.getSparkConf("Test", "local[*]");
        JavaStreamingContext context = test.getSparkContext(conf, 3);
        test.process(context, "localhost", 9999);
    }

//    public static void main(String[] args) {
//        List<Integer> list = Arrays.asList(1,2,3,4);
//        JavaSparkContext context = new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName("testApp"));
//        JavaRDD<Integer> javaRDD = context.parallelize(list);
//        Integer r = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        });
//        System.out.println(r);
//
//    }
}
