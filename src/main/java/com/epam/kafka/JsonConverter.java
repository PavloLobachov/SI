package com.epam.kafka;


import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

public class JsonConverter {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static TwitterKafkaMessage toTwitterMessage(String msg) throws JSONException {
        JSONObject jsonObject = new JSONObject(checkNotNull(msg));

        return new TwitterKafkaMessage()
                .setId(jsonObject.getLong("id"))
                .setDate(dateFormat.format(new Date(jsonObject.getString("created_at"))))
                .setText(jsonObject.getString("text"))
                .setUserName(new JSONObject(jsonObject.getString("user")).getString("name"));
    }

}
