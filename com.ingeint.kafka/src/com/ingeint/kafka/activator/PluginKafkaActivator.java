package com.ingeint.kafka.activator;

import java.util.concurrent.CountDownLatch;

import org.adempiere.plugin.utils.Incremental2PackActivator;

import com.ingeint.kafka.consumer.PartnerKafkaConsumer;
import com.ingeint.kafka.util.KeyValueLogger;

public class PluginKafkaActivator extends Incremental2PackActivator {

	private static final KeyValueLogger logger = KeyValueLogger.instance(PluginKafkaActivator.class);
	private CountDownLatch latch;
	private PartnerKafkaConsumer consumer;

	@Override
	protected void start() {
		latch = new CountDownLatch(1);
		consumer = new PartnerKafkaConsumer(latch);
		consumer.start();
	}

	@Override
	protected void stop() {
		logger.message("Stoping kafka consumer").info();

		consumer.shutdown();

		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		logger.message("Kafka has exited").info();
	}

}
