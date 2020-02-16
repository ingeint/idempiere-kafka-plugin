package com.ingeint.kafka.activator;

import java.util.concurrent.CountDownLatch;

import org.adempiere.plugin.utils.Incremental2PackActivator;

import com.ingeint.kafka.consumer.PartnerKafkaConsumer;
import com.ingeint.kafka.util.KeyValueLogger;

public class PluginKafkaActivator extends Incremental2PackActivator {

	private static final KeyValueLogger logger = KeyValueLogger.instance(PluginKafkaActivator.class);

	@Override
	protected void frameworkStarted() {
		super.frameworkStarted();

		CountDownLatch latch = new CountDownLatch(1);
		PartnerKafkaConsumer consumer = new PartnerKafkaConsumer(latch);
		consumer.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.message("Caught shutdown hook").info();

			consumer.shutdown();

			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			logger.message("Application has exited").info();
		}));
	}

}
