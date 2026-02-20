package com.example.transaction_service.service;

import org.springframework.stereotype.Service;

import com.example.transaction_service.events.TransactionCompletedEvent;
import com.example.transaction_service.events.TransactionCreatedEvent;
import com.example.transaction_service.events.TransactionEvent;
import com.example.transaction_service.events.TransactionFailedEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TransactionProducer {
    private static final Logger log = LoggerFactory.getLogger(TransactionProducer.class);
    private static final String TOPIC = "transaction-events";

    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    public TransactionProducer(KafkaTemplate<String, TransactionEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishTransactionCreated(TransactionCreatedEvent event) {
        log.info("Publishing transaction created event to Kafka: {}", event.transactionId());
        kafkaTemplate.send(TOPIC, event.transactionId().toString(), event);  // Convert UUID to String for Kafka key
    }

    public void publishTransactionCompleted(TransactionCompletedEvent event) {
        log.info("Publishing transaction completed event to Kafka: {}", event.transactionId());
        kafkaTemplate.send(TOPIC, event.transactionId().toString(), event);  // Convert UUID to String for Kafka key
    }

    public void publishTransactionFailed(TransactionFailedEvent event) {
        log.info("Publishing transaction failed event to Kafka: {}", event.transactionId());
        kafkaTemplate.send(TOPIC, event.transactionId().toString(), event);  // Convert UUID to String for Kafka key
    }
}
