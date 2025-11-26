package io.conduktor.demos.kafka.wikimedia.producer;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;

import io.conduktor.demos.kafka.wikimedia.WikimediaChangeHandler;

@Service
public class WikimediaProducer implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(WikimediaProducer.class.getSimpleName());

	private final KafkaTemplate<String, String> kafkaTemplate;

	@Value("${wikimedia.topic}")
	private String topic;

	@Value("${wikimedia.url}")
	private String wikimediaUrl;

	@Value("${wikimedia.user-agent}")
	private String userAgent;

	public WikimediaProducer(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@Override
	public void run(String... args) throws Exception {
		log.info("Starting Wikimedia stream producer...");

		// Add header, in order to follow Wikimedia policy.
		ConnectStrategy connectStrategy = ConnectStrategy.http(URI.create(wikimediaUrl))
				.header("User-Agent", userAgent);

		BackgroundEventHandler eventHandler = new WikimediaChangeHandler(kafkaTemplate, topic);
		BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(
				eventHandler,
				new EventSource.Builder(connectStrategy)
		);

		BackgroundEventSource eventSource = builder.build();

		// Start the producer in another thread.
		eventSource.start();

		log.info("Wikimedia stream producer started successfully. Topic: {}", topic);
	}

}
