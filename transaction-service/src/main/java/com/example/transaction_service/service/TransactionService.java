package com.example.transaction_service.service;

import com.example.transaction_service.model.dto.AccountReservationRequest;
import com.example.transaction_service.model.dto.CreateTransactionRequest;
import com.example.transaction_service.events.AccountCommitEvent;
import com.example.transaction_service.events.AccountReservationEvent;
import com.example.transaction_service.events.TransactionCompletedEvent;
import com.example.transaction_service.events.TransactionCreatedEvent;
import com.example.transaction_service.events.TransactionEvent;
import com.example.transaction_service.events.TransactionEventOLD;
import com.example.transaction_service.events.TransactionFailedEvent;
import com.example.transaction_service.model.SagaState;
import com.example.transaction_service.model.Transaction;
import com.example.transaction_service.model.TransactionStatus;
import com.example.transaction_service.repository.TransactionRepository;
import com.example.transaction_service.service.TransactionProducer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.time.Instant;
import java.util.UUID;


@Service
public class TransactionService {

    private static final Logger log = LoggerFactory.getLogger(TransactionService.class);

    private final TransactionRepository transactionRepository;
    private final RestClient accountsRestClient;
    private final TransactionProducer transactionProducer;

    public TransactionService(TransactionRepository transactionRepository,
                RestClient accountsRestClient, TransactionProducer transactionProducer) {
        this.transactionRepository = transactionRepository;
        this.accountsRestClient = accountsRestClient;
        this.transactionProducer = transactionProducer;
    }

    public Transaction createTransaction(CreateTransactionRequest request) {
        // Transaction constructor auto-generates the UUID
        Transaction transaction = new Transaction();
        transaction.setSourceAccountId(request.getSourceAccountId().toString());
        transaction.setDestinationAccountId(request.getDestinationAccountId().toString());
        transaction.setAmount(request.getAmount());
        transaction.setStatus(TransactionStatus.PENDING.name());
        transaction.setSagaState(SagaState.STARTED.name());
        transaction.setCreatedAt(Instant.now());
        transaction.setUpdatedAt(Instant.now());
        transaction.setVersion(0);

        transactionRepository.save(transaction);
        log.info("Transaction {} created with saga STARTED", transaction.getId());

        updateSagaState(transaction, SagaState.ACCOUNT_RESERVATION_REQUESTED);

        AccountReservationRequest reservationRequest = new AccountReservationRequest(
                UUID.fromString(transaction.getId()),
                request.getSourceAccountId(),
                request.getDestinationAccountId(),
                transaction.getAmount()
        );

        try {
            log.info("Calling accounts service to reserve funds for transaction {}", transaction.getId());
            ResponseEntity<Void> reserveResponse = accountsRestClient.post()
                    .uri("/api/v1/accounts/reserve")
                    .body(reservationRequest)
                    .retrieve()
                    .toBodilessEntity();

            if (!reserveResponse.getStatusCode().is2xxSuccessful()) {
                return compensate(transaction, reservationRequest, "Account reservation failed");
            }

            updateSagaState(transaction, SagaState.ACCOUNT_RESERVATION_SUCCESS);
            log.info("Account reservation succeeded for transaction {}", transaction.getId());

            boolean compliancePassed = runComplianceChecks(transaction);

            if (!compliancePassed) {
                release(transaction, reservationRequest);
                return fail(transaction, "Compliance checks failed");
            }

            updateSagaState(transaction, SagaState.ACCOUNT_COMMIT_REQUESTED);

            log.info("Calling accounts service to commit transaction {}", transaction.getId());
            ResponseEntity<Void> commitResponse = accountsRestClient.post()
                    .uri("/api/v1/accounts/commit")
                    .body(reservationRequest)
                    .retrieve()
                    .toBodilessEntity();

            if (!commitResponse.getStatusCode().is2xxSuccessful()) {
                release(transaction, reservationRequest);
                return fail(transaction, "Account commit failed");
            }

            transaction.setSagaState(SagaState.COMPLETED.name());
            transaction.setStatus(TransactionStatus.COMPLETED.name());
            transaction.setUpdatedAt(Instant.now());
            transaction.setVersion(transaction.getVersion() + 1);
            transactionRepository.save(transaction);
            log.info("Transaction {} COMPLETED", transaction.getId());

            return transaction;

        } catch (Exception e) {
            log.error("Error processing transaction {}: {}", transaction.getId(), e.getMessage());
            return compensate(transaction, reservationRequest, e.getMessage());
        }
    }
    public Transaction createTransactionInitial(CreateTransactionRequest request) {
        // Create transaction with STARTED saga, PENDING status - don't process
        Transaction transaction = new Transaction();
        transaction.setSourceAccountId(request.getSourceAccountId().toString());
        transaction.setDestinationAccountId(request.getDestinationAccountId().toString());
        transaction.setAmount(request.getAmount());
        transaction.setStatus(TransactionStatus.PENDING.name());
        transaction.setSagaState(SagaState.STARTED.name());
        transaction.setCreatedAt(Instant.now());
        transaction.setUpdatedAt(Instant.now());
        transaction.setVersion(0);

        transactionRepository.save(transaction);
        // publish Event
        log.info("Transaction {} created (async - awaiting Kafka processing)", transaction.getId());
        
        // Publish to Kafka instead of Spring events
        TransactionCreatedEvent event = new TransactionCreatedEvent(
            UUID.fromString(transaction.getId()),
            request.getSourceAccountId(),
            request.getDestinationAccountId(),
            transaction.getAmount()
        );
        
        transactionProducer.publishTransactionCreated(event);
        return transaction;
    }
    public void handleReservationResponse(AccountReservationEvent event) {
    log.info("Transaction Service - handleReservationResponse for transaction {}", event.transactionId());
    
    // Get transaction from Spanner DB
    Transaction transaction = transactionRepository.findById(event.transactionId().toString())
            .orElseThrow(() -> new RuntimeException("Transaction not found: " + event.transactionId()));

    // Check if reservation was successful
    if (!"SUCCESS".equals(event.status())) {
        log.error("Reservation failed for transaction {}: {}", event.transactionId(), event.message());
        
        // Mark as compensated and failed
        transaction.setSagaState(SagaState.COMPENSATED.name());
        transaction.setStatus(TransactionStatus.FAILED.name());
        transaction.setFailureReason("Account reservation failed: " + event.message());
        transaction.setUpdatedAt(Instant.now());
        transaction.setVersion(transaction.getVersion() + 1);
        transactionRepository.save(transaction);
        
        return;
    }

    // Reservation succeeded - update saga state
    updateSagaState(transaction, SagaState.ACCOUNT_RESERVATION_SUCCESS);
    log.info("Account reservation succeeded for transaction {}", transaction.getId());

    // Run compliance checks
    boolean compliancePassed = runComplianceChecks(transaction);

    if (compliancePassed) {
        log.info("Compliance checks passed for transaction {}", transaction.getId());
        
        // Update saga state to commit requested
        updateSagaState(transaction, SagaState.ACCOUNT_COMMIT_REQUESTED);

        // Publish commit request event to Kafka
        TransactionCompletedEvent commitEvent = new TransactionCompletedEvent(
            UUID.fromString(transaction.getId()),
            UUID.fromString(transaction.getSourceAccountId()),
            UUID.fromString(transaction.getDestinationAccountId()),
            transaction.getAmount()
        );
        
        transactionProducer.publishTransactionCompleted(commitEvent);
        
    } else {
        log.warn("Compliance checks failed for transaction {}", transaction.getId());
        
        // Update saga to compensated
  
        transaction.setSagaState(SagaState.COMPENSATED.name());
        transaction.setStatus(TransactionStatus.FAILED.name());
        transaction.setFailureReason("Compliance checks failed");
        transaction.setUpdatedAt(Instant.now());
        transaction.setVersion(transaction.getVersion() + 1);
        transactionRepository.save(transaction);

        // Publish release request event to release reserved funds
        TransactionFailedEvent releaseEvent = new TransactionFailedEvent(
            UUID.fromString(transaction.getId()),
            UUID.fromString(transaction.getSourceAccountId()),
            transaction.getAmount(),
            transaction.getFailureReason(),
            transaction.getUpdatedAt()
        );
        
        transactionProducer.publishTransactionFailed(releaseEvent);
    }
}

    public void handleCommitResponse(AccountCommitEvent event) {
        log.info("Transaction Service - handleCommitResponse");
         // Get transaction from Spanner DB
        Transaction transaction = transactionRepository.findById(event.transactionId().toString())
            .orElseThrow(() -> new RuntimeException("Transaction not found: " + event.transactionId()));
        
        transaction.setSagaState(SagaState.COMPLETED.name());
            transaction.setStatus(TransactionStatus.COMPLETED.name());
            transaction.setUpdatedAt(Instant.now());
            transaction.setVersion(transaction.getVersion() + 1);
            transactionRepository.save(transaction);
            log.info("Transaction {} COMPLETED", transaction.getId());

    }

    public Iterable<Transaction> getAllTransactions() {
        return transactionRepository.findAll();
    }
    private boolean runComplianceChecks(Transaction transaction) {
        return true;
    }

    private void release(Transaction transaction, AccountReservationRequest reservationRequest) {
        try {
            accountsRestClient.post()
                    .uri("/api/v1/accounts/release")
                    .body(reservationRequest)
                    .retrieve()
                    .toBodilessEntity();
        } catch (Exception e) {
            log.error("Failed to release funds for transaction {}: {}", transaction.getId(), e.getMessage());
        }
    }

    private Transaction compensate(Transaction transaction, AccountReservationRequest reservationRequest, String reason) {
        release(transaction, reservationRequest);
        return fail(transaction, reason);
    }

    private Transaction fail(Transaction transaction, String reason) {
        transaction.setSagaState(SagaState.COMPENSATED.name());
        transaction.setStatus(TransactionStatus.FAILED.name());
        transaction.setFailureReason(reason);
        transaction.setUpdatedAt(Instant.now());
        transaction.setVersion(transaction.getVersion() + 1);
        transactionRepository.save(transaction);
        log.info("Transaction {} FAILED: {}", transaction.getId(), reason);
        return transaction;
    }

    private void updateSagaState(Transaction transaction, SagaState sagaState) {
        transaction.setSagaState(sagaState.name());
        transaction.setUpdatedAt(Instant.now());
        transaction.setVersion(transaction.getVersion() + 1);
        transactionRepository.save(transaction);
        log.info("Transaction {} saga updated to {}", transaction.getId(), sagaState);
    }
}
