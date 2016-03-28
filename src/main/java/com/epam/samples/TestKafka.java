package com.epam.samples;

import java.io.Serializable;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import java.util.Properties;
import kafka.javaapi.producer.*;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

class SimplePartitioner implements Partitioner {
    public SimplePartitioner (VerifiableProperties props) {

    }

    public int partition(Object key, int a_numPartitions) {
        int partition = 0;
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
            partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions;
        }
        return partition;
    }

}

public class TestKafka implements Serializable {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        long events = 10;
        Random rnd = new Random();

        Properties prop = new Properties();
        prop.put("metadata.broker.list", "localhost:9092");
        prop.put("serializer.class","kafka.serializer.StringEncoder");
        prop.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(prop);
        Producer<String, String> producer = new Producer<String, String>(config);

        for (long nEvents = 1; nEvents < events; nEvents++) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ",www.example.com," + ip;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("kafka_test", ip, msg);
            producer.send(data);
        }
        producer.close();

    }

}
