package com.epam.twitter;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import java.util.Properties;

public class TwitterProducer {
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);


    /**
     * Information necessary for accessing the Twitter API
     */
    private static final String topic = "kafka_test";
    private static final String consumerKey = "2CBB4gO9jzozWn7rdd0FjrCNA";
    private static final String consumerSecret = "NgN8O5pHTVyVIURRzK1jgDcZ3voPbMbcVFtqmQkKZtPD6wm0wx";
    private static final String accessToken = "3709226427-qS0sPHifweQe8T1AhVel0q945h6LW5yU9UvPrT7";
    private static final String accessTokenSecret = "UfE3pYwD6hORVx4GIJHMcI6GGmX5SG43wnFELRxdD1XfL";

    /**
     * The actual Twitter stream. It's set up to collect raw JSON data
     */
    private TwitterStream twitterStream;


    private void start() throws TwitterException {

        /** Producer properties **/
        Properties props = getProducerProperties();
        ProducerConfig config = new ProducerConfig(props);
        final Producer<String, String> producer = new Producer<String, String>(config);
        ConfigurationBuilder cb = getConfigurationBuilder();


        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        /** Twitter listener **/
        UserStreamListener userStreamListener = new UserStreamListener() {
            @Override
            public void onDeletionNotice(long l, long l1) {

            }

            @Override
            public void onFriendList(long[] longs) {

            }

            @Override
            public void onFavorite(User user, User user1, Status status) {

            }

            @Override
            public void onUnfavorite(User user, User user1, Status status) {

            }

            @Override
            public void onFollow(User user, User user1) {

            }

            @Override
            public void onUnfollow(User user, User user1) {

            }

            @Override
            public void onDirectMessage(DirectMessage directMessage) {

            }

            @Override
            public void onUserListMemberAddition(User user, User user1, UserList userList) {

            }

            @Override
            public void onUserListMemberDeletion(User user, User user1, UserList userList) {

            }

            @Override
            public void onUserListSubscription(User user, User user1, UserList userList) {

            }

            @Override
            public void onUserListUnsubscription(User user, User user1, UserList userList) {

            }

            @Override
            public void onUserListCreation(User user, UserList userList) {

            }

            @Override
            public void onUserListUpdate(User user, UserList userList) {

            }

            @Override
            public void onUserListDeletion(User user, UserList userList) {

            }

            @Override
            public void onUserProfileUpdate(User user) {

            }

            @Override
            public void onBlock(User user, User user1) {

            }

            @Override
            public void onUnblock(User user, User user1) {

            }

            @Override
            public void onStatus(Status status) {
                // The EventBuilder is used to build an event using the
                // the raw JSON of a tweet
                System.out.println("------------------------------------------------------------------");
                System.out.println(status.getUser().getName() + " : " + status.getText());
                System.out.println("------------------------------------------------------------------");
                logger.info(status.getUser().getScreenName() + ": " + status.getText());

                KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, DataObjectFactory.getRawJSON(status));
//                KeyedMessage<String, String> data = new KeyedMessage<>(topic, status.getText());
                producer.send(data);

            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {

            }
        };

        twitterStream.addListener(userStreamListener);
        twitterStream.user();
    }

    private Properties getProducerProperties() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        return props;
    }

    private ConfigurationBuilder getConfigurationBuilder() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(accessToken);
        cb.setOAuthAccessTokenSecret(accessTokenSecret);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);
        return cb;
    }

    public static void main(String[] args) {

        try {
            TwitterProducer tp = new TwitterProducer();
            tp.start();

        } catch (Exception e) {
            System.err.println(e.getMessage());
            logger.info(e.getMessage());
        }

    }
}
