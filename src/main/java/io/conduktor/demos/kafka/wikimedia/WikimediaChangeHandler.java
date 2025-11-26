package io.conduktor.demos.kafka.wikimedia;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;

public class WikimediaChangeHandler implements BackgroundEventHandler {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final String topic;

	private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

	public WikimediaChangeHandler(KafkaTemplate<String, String> kafkaTemplate, String topic) {
		this.kafkaTemplate = kafkaTemplate;
		this.topic = topic;
	}

	@Override
	public void onOpen() {
		log.info("Stream connection opened");
	}

	@Override
	public void onClosed() {
		log.info("Stream connection closed");
	}

	@Override
	public void onMessage(String event, MessageEvent messageEvent) throws Exception {
		log.info(messageEvent.getData());

		// Asynchronous send using KafkaTemplate
		kafkaTemplate.send(topic, messageEvent.getData());
	}

	@Override
	public void onComment(String comment) throws Exception {
		// Nothing here
	}

	@Override
	public void onError(Throwable t) {
		log.error("Error in Stream Reading", t);
	}

}
