package com.epam.streaming;

import com.epam.hbase.HbaseClient;
import com.epam.kafka.JsonConverter;
import com.epam.kafka.TwitterKafkaMessage;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.hbase.TableName;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

public class SparkKafkaStreaming implements Serializable {
    private static final Pattern SPACE = Pattern.compile(" ");

    public SparkConf getSparkConf(String appName, String master) {
        return new SparkConf().setAppName(appName).setMaster(master);
    }

    public JavaStreamingContext getSparkContext(SparkConf sparkConf, int duration) {
        return new JavaStreamingContext(sparkConf, Durations.seconds(duration));
    }

    public void process(JavaStreamingContext jssc) {
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        HashSet<String> topicsSet = new HashSet<String>();
        topicsSet.add("kafka_test");
        kafkaParams.put("metadata.broker.list", "localhost:9092");

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        lines.print();

        lines.foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> rdd) throws Exception {
                rdd.foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String twitt) throws Exception {
                        TwitterKafkaMessage twitterKafkaMessage = JsonConverter.toTwitterMessage(twitt);

                        Timestamp currentTimestamp = new Timestamp(Calendar.getInstance().getTime().getTime());
                        long time = Long.MAX_VALUE - currentTimestamp.getTime();

                        HbaseClient hbaseClient = new HbaseClient();
                        TableName tableName = TableName.valueOf("twitter_msg");
                        hbaseClient.putTwitt(
                                tableName,
                                "data",
                                String.format("%1$s:%2$s", twitterKafkaMessage.getId(), time),
                                twitterKafkaMessage.getId(),
                                twitterKafkaMessage.getDate(),
                                twitterKafkaMessage.getText(),
                                twitterKafkaMessage.getUserName()
                                );
                    }
                });
                return null;
            }
        });


//        JavaDStream<String> words = lines.flatMap(
//                new FlatMapFunction<String, String>() {
//            @Override
//            public Iterable<String> call(String x) {
//                return Lists.newArrayList(SPACE.split(x));
//            }
//        });
//        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
//                new PairFunction<String, String, Integer>() {
//                    @Override
//                    public Tuple2<String, Integer> call(String s) {
//                        return new Tuple2<String, Integer>(s, 1);
//                    }
//                }).reduceByKey(
//                new Function2<Integer, Integer, Integer>() {
//                    @Override
//                    public Integer call(Integer i1, Integer i2) {
//                        return i1 + i2;
//                    }
//                });
//        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

    public static void main(String[] args) {
        SparkKafkaStreaming test = new SparkKafkaStreaming();
        SparkConf conf = test.getSparkConf("Test", "local[*]");
        JavaStreamingContext context = test.getSparkContext(conf, 3);


        test.process(context);
    }

}
