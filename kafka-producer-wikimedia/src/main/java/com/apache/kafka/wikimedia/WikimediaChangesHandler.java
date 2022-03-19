package com.apache.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangesHandler implements EventHandler {

    final Logger log = LoggerFactory.getLogger(WikimediaChangesHandler.class.getName());
    String topic;
    KafkaProducer<String, String> kafkaProducer;

    WikimediaChangesHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        //nothing here
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        log.info(messageEvent.getData());
        //asynchronous
        kafkaProducer.send(new ProducerRecord<String, String>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) {
        //nothing here
    }

    @Override
    public void onError(Throwable t) {
        log.error(t.getLocalizedMessage());
    }
}
