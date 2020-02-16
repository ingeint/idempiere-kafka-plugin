package com.ingeint.kafka.producer;

import java.util.Properties;
import java.util.logging.Level;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.ingeint.kafka.presenter.Partner;
import com.ingeint.kafka.util.KeyValueLogger;

public class PartnerKafkaProducer {
	private static final String KAFKA_PARTNER_TOPIC = "partner";
	private static final String KAFKA_BOOTSTRAP = "127.0.0.1:9093";

	private static final KeyValueLogger keyValueLogger = KeyValueLogger.instance(PartnerKafkaProducer.class);

	public void send(Partner partner) {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);

		KafkaProducer<String, Partner> producer = new KafkaProducer<>(properties, new StringSerializer(), new JsonSerializer<Partner>());
		ProducerRecord<String, Partner> record = new ProducerRecord<>(KAFKA_PARTNER_TOPIC, partner);
		producer.send(record, (metadata, exception) -> {

			KeyValueLogger logger = keyValueLogger.add("partition", metadata.partition()).add("offset", metadata.offset()).add("topic", metadata.topic()).add("partner", record.value());
			if (exception == null) {
				logger.level(Level.INFO).info();
			} else {
				logger.level(Level.SEVERE).exceptionWithStackTrace(exception).severe();
			}

		});
		producer.flush();
		producer.close();
	}
}
