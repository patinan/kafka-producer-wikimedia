package io.conduktor.demos.kafka.wikimedia;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;

/**
 * Test producer with Wikimedia.
 * start docker-compose of conduktor-platform (Kafka and conduktor) before run this class.
 */
public class WikimediaChangesProducer {

	private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
	private static final String TOPIC = "wikimedia.recentchange";
	private static final String WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
	private static final String WIKIMEDIA_USER_AGENT = "WikimediaKafkaProducer/1.0 (patinan@gmail.com)";

	public static void main(String[] args) {

		// Create producer properties.
		Properties properties = new Properties();

		// Connect to localhost.
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

		// Set producer properties.
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Set safe producer configurations when Kafka version <= 2.8
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Prevent from duplicate data
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // Same as setting -1, prevent from loss data
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

		// Set high throughput producer configurations.
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // Wait for 20 ms to fulfill producer batch.
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // Add producer batch size to 32KB.
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // Set producer compression type to snappy.

		// Create a producer.
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		// Add header, in order to follow Wikimedia policy.
		ConnectStrategy connectStrategy = ConnectStrategy.http(URI.create(WIKIMEDIA_URL))
				.header("User-Agent", WIKIMEDIA_USER_AGENT);

		BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, TOPIC);
		BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(
				eventHandler,
				new EventSource.Builder(connectStrategy)
		);

		BackgroundEventSource eventSource = builder.build();

		// Start the producer in another thread.
		eventSource.start();

		// Produce for 10 minutes and block the program until then.
		try {
			TimeUnit.MINUTES.sleep(10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
