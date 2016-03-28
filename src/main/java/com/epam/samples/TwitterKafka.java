package com.epam.samples;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.javaapi.producer.*;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterKafka {
    private static final String topic = "kafka_test";
    private static final String consumerKey = "2CBB4gO9jzozWn7rdd0FjrCNA";
    private static final String consumerSecret = "NgN8O5pHTVyVIURRzK1jgDcZ3voPbMbcVFtqmQkKZtPD6wm0wx";
    private static final String token = "3709226427-qS0sPHifweQe8T1AhVel0q945h6LW5yU9UvPrT7";
    private static final String secret = "UfE3pYwD6hORVx4GIJHMcI6GGmX5SG43wnFELRxdD1XfL";

    public static void run() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100);

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
// add some track terms

        endpoint.trackTerms(Lists.newArrayList("twitterapi", "#AAPSweep"));
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

// Authentication auth = new BasicAuth(username, password);
// Create a new BasicClient. By default gzip is enabled.
        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();
// Establish a connection
        client.connect();
// Do whatever needs to be done with messages


        for (int msgRead = 0; msgRead < 10; msgRead++) {
            KeyedMessage<String, String> message = null;
            try {
                message = new KeyedMessage<String, String>(topic, queue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(message);
        }
        producer.close();
        client.stop();
    }

    public static void main(String[] args) {

        try {
            TwitterKafka.run();
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }

}
