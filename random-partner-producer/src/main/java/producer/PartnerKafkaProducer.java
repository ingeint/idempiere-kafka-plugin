package producer;

import java.util.Properties;
import java.util.logging.Level;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import presenter.Partner;

public class PartnerKafkaProducer {
	private static final String KAFKA_PARTNER_TOPIC = "partnerFromExternalProducer";
	private static final String KAFKA_BOOTSTRAP = "127.0.0.1:9093";

	private static final Logger logger = LoggerFactory.getLogger(PartnerKafkaProducer.class.getName());

	public void send(Partner partner) {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);

		KafkaProducer<String, Partner> producer = new KafkaProducer<>(properties, new StringSerializer(), new JsonSerializer<Partner>());
		ProducerRecord<String, Partner> record = new ProducerRecord<>(KAFKA_PARTNER_TOPIC, partner);
		producer.send(record, (metadata, exception) -> {

			if (exception == null) {
				logger.info(record.toString());
			} else {
				logger.error(record.toString(), exception);
			}

		});
		producer.flush();
		producer.close();
	}
}
