package com.example.account_service.service;

import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.example.account_service.events.AccountCommitEvent;
import com.example.account_service.events.AccountReleaseEvent;
import com.example.account_service.events.AccountReservationEvent;
import com.example.account_service.model.dto.AccountCommitResponse;
import com.example.account_service.model.dto.AccountReleaseResponse;
import com.example.account_service.model.dto.AccountReservationResponse;

import org.slf4j.Logger;

@Service
public class AccountProducer {
    private static final Logger log = LoggerFactory.getLogger(AccountProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public AccountProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishReservationResponse(AccountReservationResponse response) {
        log.info("Publishing reservation response for transaction {}", response.transactionId());
        
        AccountReservationEvent event = AccountReservationEvent.fromResponse(response);
        
        kafkaTemplate.send("account-events", 
            response.transactionId().toString(), event);
    }

    public void publishCommitResponse(AccountCommitResponse response) {
        log.info("Publishing commit response for transaction {}", response.transactionId());
        
        AccountCommitEvent event = AccountCommitEvent.fromResponse(response);
        kafkaTemplate.send("account-events", 
            response.transactionId().toString(), event);
    }
    public void publishReleaseResponse(AccountReleaseResponse response) {
        log.info("Publishing commit response for transaction {}", response.transactionId());
        
        AccountReleaseEvent event = AccountReleaseEvent.fromResponse(response);
        kafkaTemplate.send("account-events", 
            response.transactionId().toString(), event);
    }
}
