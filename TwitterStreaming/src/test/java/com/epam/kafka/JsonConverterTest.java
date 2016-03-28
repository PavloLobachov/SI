package com.epam.kafka;

import org.codehaus.jettison.json.JSONException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JsonConverterTest {

    public static final String MSG = "{\n" +
            "  \"extended_entities\": {\n" +
            "    \"media\": [\n" +
            "      {\n" +
            "        \"display_url\": \"pic.twitter.com/1FU5zGBgOw\",\n" +
            "        \"source_user_id\": 267283568,\n" +
            "        \"type\": \"photo\",\n" +
            "        \"media_url\": \"http://pbs.twimg.com/media/CX2WizaUkAEP9FH.png\",\n" +
            "        \"source_status_id\": 683868243157618700,\n" +
            "        \"url\": \"https://t.co/1FU5zGBgOw\",\n" +
            "        \"indices\": [\n" +
            "          91,\n" +
            "          114\n" +
            "        ],\n" +
            "        \"sizes\": {\n" +
            "          \"small\": {\n" +
            "            \"w\": 340,\n" +
            "            \"h\": 170,\n" +
            "            \"resize\": \"fit\"\n" +
            "          },\n" +
            "          \"large\": {\n" +
            "            \"w\": 640,\n" +
            "            \"h\": 320,\n" +
            "            \"resize\": \"fit\"\n" +
            "          },\n" +
            "          \"thumb\": {\n" +
            "            \"w\": 150,\n" +
            "            \"h\": 150,\n" +
            "            \"resize\": \"crop\"\n" +
            "          },\n" +
            "          \"medium\": {\n" +
            "            \"w\": 600,\n" +
            "            \"h\": 300,\n" +
            "            \"resize\": \"fit\"\n" +
            "          }\n" +
            "        },\n" +
            "        \"id_str\": \"683868243090509825\",\n" +
            "        \"expanded_url\": \"http://twitter.com/IBMbigdata/status/683868243157618688/photo/1\",\n" +
            "        \"source_status_id_str\": \"683868243157618688\",\n" +
            "        \"media_url_https\": \"https://pbs.twimg.com/media/CX2WizaUkAEP9FH.png\",\n" +
            "        \"id\": 683868243090509800,\n" +
            "        \"source_user_id_str\": \"267283568\"\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "  \"in_reply_to_status_id_str\": null,\n" +
            "  \"in_reply_to_status_id\": null,\n" +
            "  \"created_at\": \"Mon Jan 04 13:02:54 +0000 2016\",\n" +
            "  \"in_reply_to_user_id_str\": null,\n" +
            "  \"source\": \"<a href=\\\"http://twitter.com\\\" rel=\\\"nofollow\\\">Twitter Web Client</a>\",\n" +
            "  \"retweeted_status\": {\n" +
            "    \"extended_entities\": {\n" +
            "      \"media\": [\n" +
            "        {\n" +
            "          \"display_url\": \"pic.twitter.com/1FU5zGBgOw\",\n" +
            "          \"indices\": [\n" +
            "            75,\n" +
            "            98\n" +
            "          ],\n" +
            "          \"sizes\": {\n" +
            "            \"small\": {\n" +
            "              \"w\": 340,\n" +
            "              \"h\": 170,\n" +
            "              \"resize\": \"fit\"\n" +
            "            },\n" +
            "            \"large\": {\n" +
            "              \"w\": 640,\n" +
            "              \"h\": 320,\n" +
            "              \"resize\": \"fit\"\n" +
            "            },\n" +
            "            \"thumb\": {\n" +
            "              \"w\": 150,\n" +
            "              \"h\": 150,\n" +
            "              \"resize\": \"crop\"\n" +
            "            },\n" +
            "            \"medium\": {\n" +
            "              \"w\": 600,\n" +
            "              \"h\": 300,\n" +
            "              \"resize\": \"fit\"\n" +
            "            }\n" +
            "          },\n" +
            "          \"id_str\": \"683868243090509825\",\n" +
            "          \"expanded_url\": \"http://twitter.com/IBMbigdata/status/683868243157618688/photo/1\",\n" +
            "          \"media_url_https\": \"https://pbs.twimg.com/media/CX2WizaUkAEP9FH.png\",\n" +
            "          \"id\": 683868243090509800,\n" +
            "          \"type\": \"photo\",\n" +
            "          \"media_url\": \"http://pbs.twimg.com/media/CX2WizaUkAEP9FH.png\",\n" +
            "          \"url\": \"https://t.co/1FU5zGBgOw\"\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    \"in_reply_to_status_id_str\": null,\n" +
            "    \"in_reply_to_status_id\": null,\n" +
            "    \"created_at\": \"Mon Jan 04 04:31:09 +0000 2016\",\n" +
            "    \"in_reply_to_user_id_str\": null,\n" +
            "    \"source\": \"<a href=\\\"http://login.voicestorm.com\\\" rel=\\\"nofollow\\\">VoiceStorm</a>\",\n" +
            "    \"retweet_count\": 20,\n" +
            "    \"retweeted\": false,\n" +
            "    \"geo\": null,\n" +
            "    \"filter_level\": \"low\",\n" +
            "    \"in_reply_to_screen_name\": null,\n" +
            "    \"is_quote_status\": false,\n" +
            "    \"id_str\": \"683868243157618688\",\n" +
            "    \"in_reply_to_user_id\": null,\n" +
            "    \"favorite_count\": 13,\n" +
            "    \"id\": 683868243157618700,\n" +
            "    \"text\": \"Detect complex events from a real-time data stream https://t.co/PMTyyhFzvp https://t.co/1FU5zGBgOw\",\n" +
            "    \"place\": null,\n" +
            "    \"lang\": \"en\",\n" +
            "    \"favorited\": false,\n" +
            "    \"possibly_sensitive\": false,\n" +
            "    \"coordinates\": null,\n" +
            "    \"truncated\": false,\n" +
            "    \"entities\": {\n" +
            "      \"urls\": [\n" +
            "        {\n" +
            "          \"display_url\": \"bit.ly/1PagxYB\",\n" +
            "          \"indices\": [\n" +
            "            51,\n" +
            "            74\n" +
            "          ],\n" +
            "          \"expanded_url\": \"http://bit.ly/1PagxYB\",\n" +
            "          \"url\": \"https://t.co/PMTyyhFzvp\"\n" +
            "        }\n" +
            "      ],\n" +
            "      \"hashtags\": [],\n" +
            "      \"media\": [\n" +
            "        {\n" +
            "          \"display_url\": \"pic.twitter.com/1FU5zGBgOw\",\n" +
            "          \"indices\": [\n" +
            "            75,\n" +
            "            98\n" +
            "          ],\n" +
            "          \"sizes\": {\n" +
            "            \"small\": {\n" +
            "              \"w\": 340,\n" +
            "              \"h\": 170,\n" +
            "              \"resize\": \"fit\"\n" +
            "            },\n" +
            "            \"large\": {\n" +
            "              \"w\": 640,\n" +
            "              \"h\": 320,\n" +
            "              \"resize\": \"fit\"\n" +
            "            },\n" +
            "            \"thumb\": {\n" +
            "              \"w\": 150,\n" +
            "              \"h\": 150,\n" +
            "              \"resize\": \"crop\"\n" +
            "            },\n" +
            "            \"medium\": {\n" +
            "              \"w\": 600,\n" +
            "              \"h\": 300,\n" +
            "              \"resize\": \"fit\"\n" +
            "            }\n" +
            "          },\n" +
            "          \"id_str\": \"683868243090509825\",\n" +
            "          \"expanded_url\": \"http://twitter.com/IBMbigdata/status/683868243157618688/photo/1\",\n" +
            "          \"media_url_https\": \"https://pbs.twimg.com/media/CX2WizaUkAEP9FH.png\",\n" +
            "          \"id\": 683868243090509800,\n" +
            "          \"type\": \"photo\",\n" +
            "          \"media_url\": \"http://pbs.twimg.com/media/CX2WizaUkAEP9FH.png\",\n" +
            "          \"url\": \"https://t.co/1FU5zGBgOw\"\n" +
            "        }\n" +
            "      ],\n" +
            "      \"user_mentions\": [],\n" +
            "      \"symbols\": []\n" +
            "    },\n" +
            "    \"contributors\": null,\n" +
            "    \"user\": {\n" +
            "      \"utc_offset\": -28800,\n" +
            "      \"friends_count\": 1783,\n" +
            "      \"profile_image_url_https\": \"https://pbs.twimg.com/profile_images/624242395459104768/eddcOVZE_normal.jpg\",\n" +
            "      \"listed_count\": 2959,\n" +
            "      \"profile_background_image_url\": \"http://pbs.twimg.com/profile_background_images/378800000104791152/7d60993e01d8748fe56dc5372cb64b22.jpeg\",\n" +
            "      \"default_profile_image\": false,\n" +
            "      \"favourites_count\": 1385,\n" +
            "      \"description\": \"Official #IBMBigData account sharing content on platforms, tools and technologies for #bigdata. Managed by Louis T Cherian and James Kobielus.\",\n" +
            "      \"created_at\": \"Wed Mar 16 17:05:33 +0000 2011\",\n" +
            "      \"is_translator\": false,\n" +
            "      \"profile_background_image_url_https\": \"https://pbs.twimg.com/profile_background_images/378800000104791152/7d60993e01d8748fe56dc5372cb64b22.jpeg\",\n" +
            "      \"protected\": false,\n" +
            "      \"screen_name\": \"IBMbigdata\",\n" +
            "      \"id_str\": \"267283568\",\n" +
            "      \"profile_link_color\": \"0099B9\",\n" +
            "      \"id\": 267283568,\n" +
            "      \"geo_enabled\": false,\n" +
            "      \"profile_background_color\": \"009AB9\",\n" +
            "      \"lang\": \"en\",\n" +
            "      \"profile_sidebar_border_color\": \"FFFFFF\",\n" +
            "      \"profile_text_color\": \"3C3940\",\n" +
            "      \"verified\": true,\n" +
            "      \"profile_image_url\": \"http://pbs.twimg.com/profile_images/624242395459104768/eddcOVZE_normal.jpg\",\n" +
            "      \"time_zone\": \"Pacific Time (US & Canada)\",\n" +
            "      \"url\": \"http://www.ibm.com/bigdata\",\n" +
            "      \"contributors_enabled\": false,\n" +
            "      \"profile_background_tile\": false,\n" +
            "      \"profile_banner_url\": \"https://pbs.twimg.com/profile_banners/267283568/1436430601\",\n" +
            "      \"statuses_count\": 27793,\n" +
            "      \"follow_request_sent\": null,\n" +
            "      \"followers_count\": 109334,\n" +
            "      \"profile_use_background_image\": true,\n" +
            "      \"default_profile\": false,\n" +
            "      \"following\": null,\n" +
            "      \"name\": \"IBM Big Data\",\n" +
            "      \"location\": null,\n" +
            "      \"profile_sidebar_fill_color\": \"95E8EC\",\n" +
            "      \"notifications\": null\n" +
            "    }\n" +
            "  },\n" +
            "  \"retweet_count\": 0,\n" +
            "  \"retweeted\": false,\n" +
            "  \"geo\": null,\n" +
            "  \"filter_level\": \"low\",\n" +
            "  \"in_reply_to_screen_name\": null,\n" +
            "  \"is_quote_status\": false,\n" +
            "  \"id_str\": \"683997029861879809\",\n" +
            "  \"in_reply_to_user_id\": null,\n" +
            "  \"favorite_count\": 0,\n" +
            "  \"id\": 683997029861879800,\n" +
            "  \"text\": \"RT @IBMbigdata: Detect complex events from a real-time data stream https://t.co/PMTyyhFzvp https://t.co/1FU5zGBgOw\",\n" +
            "  \"place\": null,\n" +
            "  \"lang\": \"en\",\n" +
            "  \"favorited\": false,\n" +
            "  \"possibly_sensitive\": false,\n" +
            "  \"coordinates\": null,\n" +
            "  \"truncated\": false,\n" +
            "  \"timestamp_ms\": \"1451912574626\",\n" +
            "  \"entities\": {\n" +
            "    \"urls\": [\n" +
            "      {\n" +
            "        \"display_url\": \"bit.ly/1PagxYB\",\n" +
            "        \"indices\": [\n" +
            "          67,\n" +
            "          90\n" +
            "        ],\n" +
            "        \"expanded_url\": \"http://bit.ly/1PagxYB\",\n" +
            "        \"url\": \"https://t.co/PMTyyhFzvp\"\n" +
            "      }\n" +
            "    ],\n" +
            "    \"hashtags\": [],\n" +
            "    \"media\": [\n" +
            "      {\n" +
            "        \"display_url\": \"pic.twitter.com/1FU5zGBgOw\",\n" +
            "        \"source_user_id\": 267283568,\n" +
            "        \"type\": \"photo\",\n" +
            "        \"media_url\": \"http://pbs.twimg.com/media/CX2WizaUkAEP9FH.png\",\n" +
            "        \"source_status_id\": 683868243157618700,\n" +
            "        \"url\": \"https://t.co/1FU5zGBgOw\",\n" +
            "        \"indices\": [\n" +
            "          91,\n" +
            "          114\n" +
            "        ],\n" +
            "        \"sizes\": {\n" +
            "          \"small\": {\n" +
            "            \"w\": 340,\n" +
            "            \"h\": 170,\n" +
            "            \"resize\": \"fit\"\n" +
            "          },\n" +
            "          \"large\": {\n" +
            "            \"w\": 640,\n" +
            "            \"h\": 320,\n" +
            "            \"resize\": \"fit\"\n" +
            "          },\n" +
            "          \"thumb\": {\n" +
            "            \"w\": 150,\n" +
            "            \"h\": 150,\n" +
            "            \"resize\": \"crop\"\n" +
            "          },\n" +
            "          \"medium\": {\n" +
            "            \"w\": 600,\n" +
            "            \"h\": 300,\n" +
            "            \"resize\": \"fit\"\n" +
            "          }\n" +
            "        },\n" +
            "        \"id_str\": \"683868243090509825\",\n" +
            "        \"expanded_url\": \"http://twitter.com/IBMbigdata/status/683868243157618688/photo/1\",\n" +
            "        \"source_status_id_str\": \"683868243157618688\",\n" +
            "        \"media_url_https\": \"https://pbs.twimg.com/media/CX2WizaUkAEP9FH.png\",\n" +
            "        \"id\": 683868243090509800,\n" +
            "        \"source_user_id_str\": \"267283568\"\n" +
            "      }\n" +
            "    ],\n" +
            "    \"user_mentions\": [\n" +
            "      {\n" +
            "        \"indices\": [\n" +
            "          3,\n" +
            "          14\n" +
            "        ],\n" +
            "        \"screen_name\": \"IBMbigdata\",\n" +
            "        \"id_str\": \"267283568\",\n" +
            "        \"name\": \"IBM Big Data\",\n" +
            "        \"id\": 267283568\n" +
            "      }\n" +
            "    ],\n" +
            "    \"symbols\": []\n" +
            "  },\n" +
            "  \"contributors\": null,\n" +
            "  \"user\": {\n" +
            "    \"utc_offset\": -28800,\n" +
            "    \"friends_count\": 6,\n" +
            "    \"profile_image_url_https\": \"https://pbs.twimg.com/profile_images/669254936593469440/8Zl1BWa7_normal.jpg\",\n" +
            "    \"listed_count\": 1,\n" +
            "    \"profile_background_image_url\": \"http://abs.twimg.com/images/themes/theme1/bg.png\",\n" +
            "    \"default_profile_image\": false,\n" +
            "    \"favourites_count\": 3,\n" +
            "    \"description\": null,\n" +
            "    \"created_at\": \"Sat Sep 19 17:36:46 +0000 2015\",\n" +
            "    \"is_translator\": false,\n" +
            "    \"profile_background_image_url_https\": \"https://abs.twimg.com/images/themes/theme1/bg.png\",\n" +
            "    \"protected\": false,\n" +
            "    \"screen_name\": \"PavloLobachov\",\n" +
            "    \"id_str\": \"3709226427\",\n" +
            "    \"profile_link_color\": \"0084B4\",\n" +
            "    \"id\": 3709226427,\n" +
            "    \"geo_enabled\": false,\n" +
            "    \"profile_background_color\": \"C0DEED\",\n" +
            "    \"lang\": \"en\",\n" +
            "    \"profile_sidebar_border_color\": \"C0DEED\",\n" +
            "    \"profile_text_color\": \"333333\",\n" +
            "    \"verified\": false,\n" +
            "    \"profile_image_url\": \"http://pbs.twimg.com/profile_images/669254936593469440/8Zl1BWa7_normal.jpg\",\n" +
            "    \"time_zone\": \"Pacific Time (US & Canada)\",\n" +
            "    \"url\": null,\n" +
            "    \"contributors_enabled\": false,\n" +
            "    \"profile_background_tile\": false,\n" +
            "    \"statuses_count\": 6,\n" +
            "    \"follow_request_sent\": null,\n" +
            "    \"followers_count\": 0,\n" +
            "    \"profile_use_background_image\": true,\n" +
            "    \"default_profile\": true,\n" +
            "    \"following\": null,\n" +
            "    \"name\": \"Pavlo Lobachov\",\n" +
            "    \"location\": null,\n" +
            "    \"profile_sidebar_fill_color\": \"DDEEF6\",\n" +
            "    \"notifications\": null\n" +
            "  }\n" +
            "}";
    private TwitterKafkaMessage kafkaMessage;

    @Before
    public void setUp() throws JSONException {
        kafkaMessage = JsonConverter.toTwitterMessage(MSG);
    }

    @Test
    public void testToTwitterMessageId() throws JSONException {
        long expectedResult = 683997029861879800L;
        long actualResult = kafkaMessage.getId();
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testToTwitterMessageDate(){
        String expectedResult = "2016-01-04 15:02:54";
        String actualResult = kafkaMessage.getDate();
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testToTwitterMessageText(){
        String expectedResult = "RT @IBMbigdata: Detect complex events from a real-time data stream https://t.co/PMTyyhFzvp https://t.co/1FU5zGBgOw";
        String actualResult = kafkaMessage.getText();
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testToTwitterMessageUserName(){
        String expectedResult = "Pavlo Lobachov";
        String actualResult = kafkaMessage.getUserName();
        assertEquals(expectedResult, actualResult);
    }

}
