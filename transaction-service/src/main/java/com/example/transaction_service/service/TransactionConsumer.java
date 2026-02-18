package com.example.transaction_service.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.example.transaction_service.model.dto.CreateTransactionRequest;
import com.example.transaction_service.model.dto.TransactionEvent;


@Service
public class TransactionConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);

    private final TransactionService transactionService;

    public TransactionConsumer(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @KafkaListener(topics = "transaction-events", groupId = "transaction-service-group")
    public void consumeTransactionEvent(TransactionEvent event) {
        log.info("Consuming transaction event from Kafka: {}", event.getTransactionId());

        CreateTransactionRequest request = new CreateTransactionRequest();
        request.setSourceAccountId(event.getSourceAccountId());
        request.setDestinationAccountId(event.getDestinationAccountId());
        request.setAmount(event.getAmount());

        transactionService.createTransaction(request);
    }
}