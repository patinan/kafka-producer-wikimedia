package io.conduktor.demos.kafka.wikimedia.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import io.conduktor.demos.kafka.wikimedia.consumer.bean.WikimediaRecentChange;

@Service
public class ConsumerService {

	private static final Logger log = LoggerFactory.getLogger(ConsumerService.class.getSimpleName());

	@KafkaListener(topics = "wikimedia.recentchange", groupId = "group-wikimedia")
	public void listen(WikimediaRecentChange recentChange) {
		log.info("Received: {}", recentChange);
	}
}
