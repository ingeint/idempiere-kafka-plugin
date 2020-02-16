package com.ingeint.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.compiere.model.MBPartner;
import org.compiere.model.Query;
import org.compiere.util.Env;

import com.ingeint.kafka.presenter.Partner;
import com.ingeint.kafka.util.KeyValueLogger;

public class PartnerKafkaConsumer extends Thread {

	private static final String OFFSET_REST = "latest";
	private static final String GROUP_ID = "idempierePartnerConsumer";
	private static final String KAFKA_PARTNER_TOPIC = "partnerFromExternalProducer";
	private static final String KAFKA_BOOTSTRAP = "127.0.0.1:9093";

	private static final KeyValueLogger logger = KeyValueLogger.instance(PartnerKafkaConsumer.class);

	private CountDownLatch latch;
	private KafkaConsumer<String, Partner> consumer;

	public PartnerKafkaConsumer(CountDownLatch latch) {
		this.latch = latch;

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_REST);

		consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new JsonDeserializer<>(Partner.class));
		consumer.subscribe(Arrays.asList(KAFKA_PARTNER_TOPIC));
	}

	@Override
	public void run() {
		try {
			while (true) {
				ConsumerRecords<String, Partner> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, Partner> record : records) {
					Partner incomingPartner = record.value();
					logger.add("partition", record.partition()).add("offset", record.offset()).add("topic", record.topic()).add("partner", incomingPartner).level(Level.INFO).info();

					MBPartner partner = getPartnerFromUU(incomingPartner.getUu());

					if (partner == null) {
						partner = new MBPartner(Env.getCtx(), 0, null);
					}

					partner.setC_BPartner_UU(incomingPartner.getUu());
					partner.setName(incomingPartner.getName());
					partner.setValue(incomingPartner.getValue());
					partner.setC_BP_Group_ID(103);
					partner.setClientOrg(11, 0);
					partner.saveEx();
				}
			}
		} catch (WakeupException we) {
			logger.message("Received shutdown signal!").level(Level.INFO).info();
		} catch (Exception e) {
			logger.exceptionWithStackTrace(e).severe();
		} finally {
			consumer.close();
			latch.countDown();
		}
	}

	public void shutdown() {
		consumer.wakeup();
	}

	private MBPartner getPartnerFromUU(String uu) {
		return new Query(Env.getCtx(), MBPartner.Table_Name, MBPartner.COLUMNNAME_C_BPartner_UU + "=?", null).setParameters(uu).first();
	}

}
