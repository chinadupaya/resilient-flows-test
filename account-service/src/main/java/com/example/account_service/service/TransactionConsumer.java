package com.example.account_service.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.example.account_service.events.TransactionCompletedEvent;
import com.example.account_service.events.TransactionCreatedEvent;
import com.example.account_service.events.TransactionEvent;
import com.example.account_service.events.TransactionFailedEvent;

@Service
public class TransactionConsumer {
    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);

    private final AccountService accountService;

    public TransactionConsumer(AccountService accountService) {
        this.accountService = accountService;
    }

    @KafkaListener(topics = "transaction-events", groupId = "account-service-group")
    public void listen(TransactionEvent event) {
        log.info("Received transaction event: {}", event.eventType());
        switch (event) {
            case TransactionCreatedEvent created -> {
                // handle created
                accountService.processReservationAsync(created);
        }
        case TransactionCompletedEvent completed -> {
            // handle completed
            accountService.processCommitAsync(completed);
        }
        case TransactionFailedEvent failed -> {
            // handle failed
            accountService.processReleaseAsync(failed);
        }
    }
    }

    // @KafkaListener(topics = "account-commit-requests", groupId = "account-service-group")
    // public void handleCommitRequest(TransactionEvent event) {
    //     log.info("Received commit request for transaction {}", event.getTransactionId());
    //     accountService.processCommitAsync(event);
    // }

    // @KafkaListener(topics = "account-release-requests", groupId = "account-service-group")
    // public void handleReleaseRequest(TransactionEvent event) {
    //     log.info("Received release request for transaction {}", event.getTransactionId());
    //     accountService.processReleaseAsync(event);
    // }
}