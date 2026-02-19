package com.example.transaction_service.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.example.transaction_service.events.AccountCommitEvent;
import com.example.transaction_service.events.AccountEvent;
import com.example.transaction_service.events.AccountReservationEvent;
import com.example.transaction_service.events.TransactionEvent;
import com.example.transaction_service.model.dto.CreateTransactionRequest;


@Service
public class AccountConsumer {

    private static final Logger log = LoggerFactory.getLogger(AccountConsumer.class);

    private final TransactionService transactionService;

    public AccountConsumer(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @KafkaListener(topics = "account-events", groupId = "transaction-service-group")
    public void handleAccountEvent(AccountEvent event) {
        log.info("Received account event: {}", event.eventType());
        switch (event.eventType()) {
            case "RESERVATION_RESPONSE" -> handleReservationResponse((AccountReservationEvent) event);
            case "COMMIT_RESPONSE" -> handleCommitResponse((AccountCommitEvent) event);
            case "RELEASE_RESPONSE" -> log.info("Release confirmed for transaction");
            default -> log.warn("Unknown event type: {}", event.eventType());
        }
    }

    private void handleReservationResponse(AccountReservationEvent event) {
        log.info("Handling reservation response for transaction {}: {}", 
            event.transactionId(), event.status());
        
        // Your logic to update transaction saga state based on reservation success/failure
        transactionService.handleReservationResponse(event);
    }

    private void handleCommitResponse(AccountCommitEvent event) {
        log.info("Handling commit response for transaction {}: {}", 
            event.transactionId(), event.success());
        
        // Your logic to mark transaction as completed
        transactionService.handleCommitResponse(event);
    }
}