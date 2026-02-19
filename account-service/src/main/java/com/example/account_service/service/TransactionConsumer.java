package com.example.account_service.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.example.account_service.model.dto.TransactionEvent;


@Service
public class TransactionConsumer {
    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);

    private final AccountService accountService;

    public TransactionConsumer(AccountService accountService) {
        this.accountService = accountService;
    }

    // TransactionPending event
    @KafkaListener(topics = "transaction-events")
    public void listen(TransactionEvent event) {
        log.info("Received event: " + event.getTransactionId());
        // reserve amount

        // send event on success
    }
}
