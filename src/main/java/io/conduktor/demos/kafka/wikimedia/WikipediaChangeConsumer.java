package io.conduktor.demos.kafka.wikimedia;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Test consumer with Wikemdia.
 */
public class WikipediaChangeConsumer {

	private static final Logger log = LoggerFactory.getLogger(WikipediaChangeConsumer.class.getSimpleName());
	private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
	private static final String GROUP_ID = "consumer-wikimedia";
	private static final String TOPIC = "wikimedia.recentchange";

	public static void main(String[] args) throws IOException {

		// Create Kafka client.
		KafkaConsumer<String, String> consumer = createKafkaConsumer();
		
		// Subscribe the consumer.
		consumer.subscribe(Collections.singleton(TOPIC));
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

			int totalRecords = records.count();
			log.info("Received {} records.", totalRecords);
			
			if (totalRecords > 0) {
				for (ConsumerRecord<String, String> record : records) {
					String topic = record.topic();
					int partition =record.partition();
					long offset = record.offset();
					String vaule = record.value();

					log.info("Consumer record => topic: {} partition: {}, offset: {}", topic, partition, offset);
					log.info("Consumer record vaule: {}", vaule);
				}
			}
		}
	}

	private static KafkaConsumer<String, String> createKafkaConsumer() {

		// Create consumer properties.
		Properties properties = new Properties();

		// Connect to localhost.
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

		// Set consumer properties.
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		return new KafkaConsumer<>(properties);
	}
}
