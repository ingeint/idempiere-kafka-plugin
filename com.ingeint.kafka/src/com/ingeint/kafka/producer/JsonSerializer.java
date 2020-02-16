package com.ingeint.kafka.producer;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {

    private final Gson gson;

    public JsonSerializer() {
        gson = new Gson();
    }


    @Override
    public byte[] serialize(String topic, T data) {
        return gson.toJson(data).getBytes();
    }

}
