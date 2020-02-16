package com.ingeint.kafka.consumer;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;

public class JsonDeserializer<T> implements Deserializer<T> {
	private final Gson gson;
	private final Class<T> clazz;

	public JsonDeserializer(Class<T> clazz) {
		this.clazz = clazz;
		this.gson = new Gson();
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		return gson.fromJson(new String(data), clazz);
	}
}
