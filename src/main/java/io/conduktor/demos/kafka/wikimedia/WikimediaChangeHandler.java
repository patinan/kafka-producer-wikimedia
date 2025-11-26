package io.conduktor.demos.kafka.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;

public class WikimediaChangeHandler implements BackgroundEventHandler {

	private KafkaProducer<String, String> kafkaProducer;
	private String topic;

	private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

	public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
		this.kafkaProducer = kafkaProducer;
		this.topic = topic;
	}

	@Override
	public void onOpen() {
		// Nothing here
	}

	@Override
	public void onClosed() {
		kafkaProducer.close();
	}

	@Override
	public void onMessage(String event, MessageEvent messageEvent) throws Exception {
		log.info(messageEvent.getData());

		// Asynchronous
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageEvent.getData());
		kafkaProducer.send(record);
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
