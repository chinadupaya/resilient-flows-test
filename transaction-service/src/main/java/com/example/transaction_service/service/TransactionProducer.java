package com.example.transaction_service.service;

import org.springframework.stereotype.Service;

import com.example.transaction_service.model.dto.TransactionEvent;

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

    public void publishTransactionEvent(TransactionEvent event) {
        log.info("Publishing transaction event to Kafka: {}", event.getTransactionId());
        kafkaTemplate.send(TOPIC, event.getTransactionId().toString(), event);  // Convert UUID to String for Kafka key
    }
}
