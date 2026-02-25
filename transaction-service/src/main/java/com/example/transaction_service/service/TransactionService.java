package com.example.transaction_service.service;

import com.example.transaction_service.model.dto.AccountReservationRequest;
import com.example.transaction_service.model.dto.CreateTransactionRequest;
import com.example.transaction_service.events.AccountCommitEvent;
import com.example.transaction_service.events.AccountReservationEvent;
import com.example.transaction_service.events.TransactionCompletedEvent;
import com.example.transaction_service.events.TransactionCreatedEvent;

import com.example.transaction_service.events.TransactionFailedEvent;
import com.example.transaction_service.model.SagaState;
import com.example.transaction_service.model.Transaction;
import com.example.transaction_service.model.TransactionStatus;
import com.example.transaction_service.repository.TransactionRepository;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;



import java.time.Instant;
import java.util.UUID;


@Service
public class TransactionService {

    private static final Logger log = LoggerFactory.getLogger(TransactionService.class);

    private final TransactionRepository transactionRepository;
    private final RestClient accountsRestClient;
    private final TransactionProducer transactionProducer;

    private final Counter transactionCompletedCounter;
    private final Counter transactionFailedCounter;
    private final Counter transactionStartedCounter;

    private final AtomicInteger activeTransactions = new AtomicInteger();

    public TransactionService(TransactionRepository transactionRepository,
                RestClient accountsRestClient, TransactionProducer transactionProducer,
                MeterRegistry meterRegistry) {
        this.transactionRepository = transactionRepository;
        this.accountsRestClient = accountsRestClient;
        this.transactionProducer = transactionProducer;

        this.transactionStartedCounter =
            meterRegistry.counter("transactions.started");

        this.transactionCompletedCounter =
                meterRegistry.counter("transactions.completed");

        this.transactionFailedCounter =
                meterRegistry.counter("transactions.failed");
        meterRegistry.gauge("transactions.active", activeTransactions);
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

        transactionRepository.save(transaction);
        log.info("Transaction {} created with saga STARTED", transaction.getId());
        transactionStartedCounter.increment();
        activeTransactions.incrementAndGet();

        if (!updateSagaState(transaction,
            SagaState.STARTED,
            SagaState.ACCOUNT_RESERVATION_REQUESTED)) {
            return transaction;
        }

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
                return fail(transaction, "Account reservation failed");
            }

            if (!updateSagaState(transaction,
                SagaState.ACCOUNT_RESERVATION_REQUESTED,
                SagaState.ACCOUNT_RESERVATION_SUCCESS)) {
                return transaction;
            }

            log.info("Account reservation succeeded for transaction {}", transaction.getId());

            if (!runComplianceChecks(transaction)) {
                releaseFunds(transaction, reservationRequest);
                return transaction;
            }


            log.info("Calling accounts service to commit transaction {}", transaction.getId());
            ResponseEntity<Void> commitResponse = accountsRestClient.post()
                    .uri("/api/v1/accounts/commit")
                    .body(reservationRequest)
                    .retrieve()
                    .toBodilessEntity();
            if (!updateSagaState(transaction,
                    SagaState.ACCOUNT_RESERVATION_SUCCESS,
                    SagaState.ACCOUNT_COMMIT_REQUESTED)) {
                return transaction;
            }

            if (!commitResponse.getStatusCode().is2xxSuccessful()) {
                release(transaction, reservationRequest);
                return fail(transaction, "Account commit failed");
            }

            if (!updateSagaState(transaction,
                    SagaState.ACCOUNT_COMMIT_REQUESTED,
                    SagaState.COMPLETED)) {
                return transaction;
            }
            transaction.setStatus(TransactionStatus.COMPLETED.name());
            transactionRepository.save(transaction);

            // transaction.setVersion(transaction.getVersion() + 1);
            log.info("Transaction {} COMPLETED", transaction.getId());
            transactionCompletedCounter.increment();
            activeTransactions.decrementAndGet();
            return transaction;

        } catch (Exception e) {
            log.error("Error processing transaction {}: {}", transaction.getId(), e.getMessage());
                release(transaction, reservationRequest);
            return fail(transaction, e.getMessage());
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
        // transaction.setVersion(0);

        transactionRepository.save(transaction);

        transactionStartedCounter.increment();
        activeTransactions.incrementAndGet();

        if (!updateSagaState(transaction,
                SagaState.STARTED,
                SagaState.ACCOUNT_RESERVATION_REQUESTED)) {
            return transaction;
        }
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
        Transaction transaction = transactionRepository
            .findById(event.transactionId().toString())
            .orElseThrow(() -> new RuntimeException("Transaction not found: " + event.transactionId()));

        // check if state is correct
        if (!transaction.getSagaState()
                .equals(SagaState.ACCOUNT_RESERVATION_REQUESTED.name())) {
            log.info("Duplicate reservation event ignored for {}", transaction.getId());
            return;
        }

        // Check if reservation was successful
        if (!"SUCCESS".equals(event.status())) {
            fail(transaction, "Account reservation failed: " + event.message());
            return;
        }

        // Reservation succeeded - update saga state
        if (!updateSagaState(transaction,
                SagaState.ACCOUNT_RESERVATION_REQUESTED,
                SagaState.ACCOUNT_RESERVATION_SUCCESS)) {
            return;
        }


        // Run compliance checks
        if (!runComplianceChecks(transaction)) {
            requestCompensation(transaction, "Compliance failed");
            return;
        }

        if (!updateSagaState(transaction,
                SagaState.ACCOUNT_RESERVATION_SUCCESS,
                SagaState.ACCOUNT_COMMIT_REQUESTED)) {
            return;
        }

        TransactionCompletedEvent commitEvent =
                new TransactionCompletedEvent(
                        UUID.fromString(transaction.getId()),
                        UUID.fromString(transaction.getSourceAccountId()),
                        UUID.fromString(transaction.getDestinationAccountId()),
                        transaction.getAmount());

        transactionProducer.publishTransactionCompleted(commitEvent);

        
    }

    public void handleCommitResponse(AccountCommitEvent event) {
        log.info("Transaction Service - handleCommitResponse");
         // Get transaction from Spanner DB
        Transaction transaction = transactionRepository
            .findById(event.transactionId().toString())
            .orElseThrow(() -> new RuntimeException("Transaction not found: " + event.transactionId()));
        
        if (!updateSagaState(transaction,
                SagaState.ACCOUNT_COMMIT_REQUESTED,
                SagaState.COMPLETED)) {
            return;
        }

        transaction.setStatus(TransactionStatus.COMPLETED.name());
        transactionRepository.save(transaction);

        transactionCompletedCounter.increment();
        activeTransactions.decrementAndGet();
    }

    public Iterable<Transaction> getAllTransactions() {
        return transactionRepository.findAll();
    }
    private boolean runComplianceChecks(Transaction transaction) {
        // return false;
        return Math.random() > 0.3;
    }

    private void releaseFunds(Transaction tx, AccountReservationRequest request) {

        if (!updateSagaState(tx,
                SagaState.ACCOUNT_RESERVATION_SUCCESS,
                SagaState.COMPENSATION_REQUESTED)) {
            return; // already compensated or invalid
        }

        try {
            ResponseEntity<Void> response =
                    accountsRestClient.post()
                            .uri("/api/v1/accounts/release")
                            .body(request)
                            .retrieve()
                            .toBodilessEntity();

            if (!response.getStatusCode().is2xxSuccessful()) {
                log.error("Release failed for {}", tx.getId());
                return;
            }

            if (!updateSagaState(tx,
                    SagaState.COMPENSATION_REQUESTED,
                    SagaState.COMPENSATED)) {
                return;
            }

            tx.setStatus(TransactionStatus.FAILED.name());
            tx.setUpdatedAt(Instant.now());
            transactionRepository.save(tx);

            transactionFailedCounter.increment();
            activeTransactions.decrementAndGet();

            log.info("Transaction {} compensated successfully", tx.getId());

        } catch (Exception e) {
            log.error("Compensation error for {}: {}", tx.getId(), e.getMessage());
        }
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

    private Transaction fail(Transaction transaction, String reason) {
        transaction.setSagaState(SagaState.FAILED.name());
        transaction.setStatus(TransactionStatus.FAILED.name());
        transaction.setFailureReason(reason);
        transaction.setUpdatedAt(Instant.now());
        // transaction.setVersion(transaction.getVersion() + 1);
        transactionRepository.save(transaction);
        log.info("Transaction {} FAILED: {}", transaction.getId(), reason);
        transactionFailedCounter.increment();
        activeTransactions.decrementAndGet();
        return transaction;
    }

    private void requestCompensation(Transaction transaction, String reason) {

        if (!updateSagaState(transaction,
                SagaState.ACCOUNT_RESERVATION_SUCCESS,
                SagaState.COMPENSATION_REQUESTED)) {
            return;
        }

        transaction.setStatus(TransactionStatus.FAILED.name());
        transaction.setFailureReason(reason);
        transactionRepository.save(transaction);

        TransactionFailedEvent event =
                new TransactionFailedEvent(
                        UUID.fromString(transaction.getId()),
                        UUID.fromString(transaction.getSourceAccountId()),
                        transaction.getAmount(),
                        reason);

        transactionProducer.publishTransactionFailed(event);
    }

    private boolean updateSagaState(
        Transaction tx,
        SagaState expected,
        SagaState next) {

        if (!tx.getSagaState().equals(expected.name())) {
            log.info("Ignoring transition for {}. Expected {}, found {}",
                    tx.getId(), expected, tx.getSagaState());
            return false;
        }

        tx.setSagaState(next.name());
        tx.setUpdatedAt(Instant.now());
        transactionRepository.save(tx);

        log.info("Transaction {} transitioned {} -> {}",
                tx.getId(), expected, next);

        return true;
    }
}
