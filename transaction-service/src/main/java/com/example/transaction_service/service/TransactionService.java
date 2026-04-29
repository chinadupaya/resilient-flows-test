package com.example.transaction_service.service;

import com.example.transaction_service.model.dto.AccountReservationRequest;
import com.example.transaction_service.model.dto.CreateTransactionRequest;
import com.example.transaction_service.model.dto.TransactionReservation;
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
import io.micrometer.core.instrument.Gauge;



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
        Gauge.builder("transaction.reservations.active",
            () -> transactionRepository.countActiveReservations())
            .register(meterRegistry);
        Gauge.builder("transactions.stuck",
        () -> transactionRepository.countStuckTransactions())
        .register(meterRegistry);
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
                Transaction t = fail(transaction, "Account reservation failed");
                transactionFailedCounter.increment();
                return t;
            }

            if (!updateSagaState(transaction,
                SagaState.ACCOUNT_RESERVATION_REQUESTED,
                SagaState.ACCOUNT_RESERVATION_SUCCESS)) {
                return transaction;
            }

            chaosPoint("AFTER_RESERVE");

            log.info("Account reservation succeeded for transaction {}", transaction.getId());

            if (!runComplianceChecks(transaction)) {
                releaseFunds(transaction, reservationRequest);
                transactionFailedCounter.increment();
                return transaction;
            }

            chaosPoint("AFTER_COMPLIANCE");


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
                Transaction t = fail(transaction, "Account commit failed");
                transactionFailedCounter.increment();
                return t;
            }
            chaosPoint("AFTER_COMMIT");

            if (!updateSagaState(transaction,
                    SagaState.ACCOUNT_COMMIT_REQUESTED,
                    SagaState.COMPLETED)) {
                return transaction;
            }
            transaction.setStatus(TransactionStatus.COMPLETED.name());
            transactionRepository.save(transaction);

            log.info("Transaction {} COMPLETED", transaction.getId());
            transactionCompletedCounter.increment();
            return transaction;

        } catch (Exception e) {
            log.error("Error processing transaction {}: {}", transaction.getId(), e.getMessage());
                release(transaction, reservationRequest);
            Transaction t = fail(transaction, e.getMessage());
            transactionFailedCounter.increment();
            return t;
        }
    }
    
    public Transaction createInitialTransaction(CreateTransactionRequest request) {
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
            return transaction;
    }

    public void markReservationRequested(String transactionId) {
        Transaction transaction = getTransactionOrThrow(transactionId);
        updateSagaState(
                transaction,
                SagaState.STARTED,
                SagaState.ACCOUNT_RESERVATION_REQUESTED
        );
    }

    public boolean reserveFunds(AccountReservationRequest reservationRequest) {
        ResponseEntity<Void> reserveResponse = accountsRestClient.post()
                .uri("/api/v1/accounts/reserve")
                .body(reservationRequest)
                .retrieve()
                .toBodilessEntity();

        return reserveResponse.getStatusCode().is2xxSuccessful();
    }

    public void markReservationSuccess(String transactionId) {
        Transaction transaction = getTransactionOrThrow(transactionId);
        updateSagaState(
                transaction,
                SagaState.ACCOUNT_RESERVATION_REQUESTED,
                SagaState.ACCOUNT_RESERVATION_SUCCESS
        );
    }

    public boolean checkCompliance(String transactionId) {
        Transaction transaction = getTransactionOrThrow(transactionId);
        return runComplianceChecks(transaction);
    }

    public void markCommitRequested(String transactionId) {
        Transaction transaction = getTransactionOrThrow(transactionId);
        updateSagaState(
                transaction,
                SagaState.ACCOUNT_RESERVATION_SUCCESS,
                SagaState.ACCOUNT_COMMIT_REQUESTED
        );
    }

    public boolean commitFunds(AccountReservationRequest reservationRequest) {
        ResponseEntity<Void> commitResponse = accountsRestClient.post()
                .uri("/api/v1/accounts/commit")
                .body(reservationRequest)
                .retrieve()
                .toBodilessEntity();

        return commitResponse.getStatusCode().is2xxSuccessful();
    }

    public void releaseFunds(String transactionId, AccountReservationRequest reservationRequest) {
        Transaction transaction = getTransactionOrThrow(transactionId);
        release(transaction, reservationRequest);
    }
    

    public Transaction completeTransaction(String transactionId) {
        Transaction transaction = getTransactionOrThrow(transactionId);

        updateSagaState(
                transaction,
                SagaState.ACCOUNT_COMMIT_REQUESTED,
                SagaState.COMPLETED
        );

        transaction.setStatus(TransactionStatus.COMPLETED.name());
        transaction.setUpdatedAt(Instant.now());
        transactionRepository.save(transaction);

        log.info("Transaction {} COMPLETED", transaction.getId());
        transactionCompletedCounter.increment();

        return transaction;
    }

    public Transaction failTransaction(String transactionId, String reason) {
        Transaction transaction = getTransactionOrThrow(transactionId);
        // transactionFailedCounter.increment();
        return fail(transaction, reason);
    }
            
    public Transaction createTransactioAsync(CreateTransactionRequest request) {
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
            transactionProducer.publishTransactionFailed(
                new TransactionFailedEvent(
                    event.transactionId(),
                    event.accountId(),
                    event.reservedAmount(),
                    "Account reservation failed: " + event.message()
                )
            );
    
            return;
        }

        // Reservation succeeded - update saga state
        if (!updateSagaState(transaction,
                SagaState.ACCOUNT_RESERVATION_REQUESTED,
                SagaState.ACCOUNT_RESERVATION_SUCCESS)) {
            return;
        }

        chaosPoint("AFTER_RESERVE");


        // Run compliance checks
        if (!runComplianceChecks(transaction)) {
            requestCompensation(transaction, "Compliance failed");
            transactionFailedCounter.increment();
            return;
        }

        chaosPoint("AFTER_COMPLIANCE");

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
        chaosPoint("AFTER_COMMIT");
        
        if (!updateSagaState(transaction,
                SagaState.ACCOUNT_COMMIT_REQUESTED,
                SagaState.COMPLETED)) {
            return;
        }

        transaction.setStatus(TransactionStatus.COMPLETED.name());
        transactionRepository.save(transaction);

        transactionCompletedCounter.increment();
    }

    public Iterable<Transaction> getAllTransactions() {
        return transactionRepository.findAll();
    }
    
    public Iterable<TransactionReservation> getPendingTransactions() {
        return transactionRepository.findTransactionsWithActiveReservations()
            .stream()
            .map(t -> new TransactionReservation(
                    UUID.fromString(t.getId()),
                    UUID.fromString(t.getSourceAccountId()),
                    t.getAmount()
            ))
            .toList();
    }
    public void cancelReservationRequested(String transactionId) {
        Transaction tx = getTransactionOrThrow(transactionId);
        fail(tx, "Cancelled after reservation request");
    }

    public void compensateTransaction(String transactionId,
                                  AccountReservationRequest request) {
    Transaction tx = getTransactionOrThrow(transactionId);
        compensate(tx, request);
    }
    // PRIVATE FUNCTIONS
    private void compensate(Transaction tx, AccountReservationRequest request) {

        SagaState state = SagaState.valueOf(tx.getSagaState());

        try {
            ResponseEntity<Void> response;

            switch (state) {
                case COMPENSATION_REQUESTED:
                case ACCOUNT_RESERVATION_SUCCESS:
                case ACCOUNT_RESERVATION_REQUESTED:
                    log.info("Compensation = RELEASE for {}", tx.getId());

                    response = accountsRestClient.post()
                            .uri("/api/v1/accounts/release")
                            .body(request)
                            .retrieve()
                            .toBodilessEntity();
                    break;

                case ACCOUNT_COMMIT_REQUESTED:
                case COMPLETED:
                    log.info("Compensation = REFUND for {}", tx.getId());

                    response = accountsRestClient.post()
                            .uri("/api/v1/accounts/refund")
                            .body(request)
                            .retrieve()
                            .toBodilessEntity();
                    break;

                default:
                    log.warn("No compensation needed for state {}", state);
                    return;
            }

            if (!response.getStatusCode().is2xxSuccessful()) {
                log.error("Compensation failed for {}", tx.getId());
                return;
            }

            tx.setSagaState(SagaState.COMPENSATED.name());
            tx.setStatus(TransactionStatus.FAILED.name());
            tx.setUpdatedAt(Instant.now());
            transactionRepository.save(tx);

            // transactionFailedCounter.increment();

            log.info("Transaction {} compensated successfully", tx.getId());

        } catch (Exception e) {
            log.error("Compensation error for {}: {}", tx.getId(), e.getMessage());
        }
    }
    private boolean runComplianceChecks(Transaction transaction) {
        // return false;
        return Math.random() > 0.3;
    }

    private Transaction getTransactionOrThrow(String transactionId) {
        return transactionRepository.findById(transactionId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Transaction not found: " + transactionId
                ));
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
        transactionRepository.save(transaction);
        log.info("Transaction {} FAILED: {}", transaction.getId(), reason);
        transactionFailedCounter.increment();
        return transaction;
    }

    private void requestCompensation(Transaction transaction, String reason) {

        if (!updateSagaState(transaction,
                SagaState.ACCOUNT_RESERVATION_SUCCESS,
                SagaState.COMPENSATION_REQUESTED)) {
            return;
        }

        // transaction.setStatus(TransactionStatus.FAILED.name());
        // transaction.setFailureReason(reason);
        // transactionRepository.save(transaction);

        AccountReservationRequest request =
                new AccountReservationRequest(
                        UUID.fromString(transaction.getId()),
                        UUID.fromString(transaction.getSourceAccountId()),
                        UUID.fromString(transaction.getDestinationAccountId()),
                        transaction.getAmount()
                );

        compensate(transaction, request);
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

    private void chaosPoint(String name) {
    String chaos = System.getenv("CHAOS_POINT");
    if (chaos != null && chaos.equals(name)) {
        log.error("CHAOS: crashing at {}", name);
        Runtime.getRuntime().halt(1);
    }
}
}
