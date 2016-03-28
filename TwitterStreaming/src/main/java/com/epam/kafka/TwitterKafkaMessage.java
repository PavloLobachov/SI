package com.epam.kafka;

import java.io.Serializable;

public class TwitterKafkaMessage implements Serializable {
    private Long id;
    private String date;
    private String text;
    private String userName;

    public Long getId() {
        return id;
    }

    public TwitterKafkaMessage setId(Long id) {
        this.id = id;
        return this;
    }

    public String getDate() {
        return date;
    }

    public TwitterKafkaMessage setDate(String date) {
        this.date = date;
        return this;
    }

    public String getText() {
        return text;
    }

    public TwitterKafkaMessage setText(String text) {
        this.text = text;
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public TwitterKafkaMessage setUserName(String userName) {
        this.userName = userName;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TwitterKafkaMessage that = (TwitterKafkaMessage) o;

        if (!id.equals(that.id)) return false;
        if (!date.equals(that.date)) return false;
        if (!text.equals(that.text)) return false;
        return userName.equals(that.userName);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + date.hashCode();
        result = 31 * result + text.hashCode();
        result = 31 * result + userName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TwitterKafkaMessage{" +
                "id=" + id +
                ", date='" + date + '\'' +
                ", text='" + text + '\'' +
                ", userName='" + userName + '\'' +
                '}';
    }
}
