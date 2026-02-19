package com.example.account_service.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.example.account_service.events.TransactionEvent;

@Service
public class TransactionConsumer {
    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);

    private final AccountService accountService;

    public TransactionConsumer(AccountService accountService) {
        this.accountService = accountService;
    }

    @KafkaListener(topics = "transaction-events", groupId = "account-service-group")
    public void handleTransactionCreated(TransactionEvent event) {
        log.info("Received transaction created event: {}", event.getTransactionId());
        accountService.processReservationAsync(event);
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