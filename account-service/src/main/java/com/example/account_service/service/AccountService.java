package com.example.account_service.service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClient;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import com.example.account_service.events.TransactionCompletedEvent;
import com.example.account_service.events.TransactionCreatedEvent;
import com.example.account_service.events.TransactionFailedEvent;
import com.example.account_service.exception.AccountNotFoundException;
import com.example.account_service.exception.InsufficientBalanceException;
import com.example.account_service.model.Account;
import com.example.account_service.model.dto.AccountCommitRequest;
import com.example.account_service.model.dto.AccountCommitResponse;
import com.example.account_service.model.dto.AccountReleaseRequest;
import com.example.account_service.model.dto.AccountReleaseResponse;
import com.example.account_service.model.dto.AccountReservationRequest;
import com.example.account_service.model.dto.AccountReservationResponse;
import com.example.account_service.model.dto.TransactionReservation;
import com.example.account_service.repository.AccountRepository;

@Service
public class AccountService {

    private static final Logger log = LoggerFactory.getLogger(AccountService.class);

    private final AccountRepository accountRepository;
    private final AccountProducer accountProducer;
    private final RestClient transactionsRestClient;

    public AccountService(AccountRepository accountRepository, 
        AccountProducer accountProducer, RestClient transactionsRestClient,
    MeterRegistry meterRegistry) {
        this.accountRepository = accountRepository;
        this.accountProducer = accountProducer;
        this.transactionsRestClient = transactionsRestClient;
        
        // Gauge.builder("accounts.money.total",
        //     accountRepository,
        //     repo -> repo.totalSystemMoney().doubleValue())
        //     .register(meterRegistry);
        // Gauge.builder("accounts.reserved.total",
        //     accountRepository,
        //     repo -> repo.totalReserved().doubleValue())
        // .register(meterRegistry);
    }

    // ============================================================================
    // ACCOUNT MANAGEMENT (GENERAL)
    // ============================================================================

    public Account createAccount(String accountHolderName) {
        Account account = Account.builder()
                .accountHolderName(accountHolderName)
                .balance(BigDecimal.ZERO)
                .reservedAmount(BigDecimal.ZERO)
                .build();
        return accountRepository.save(account);
    }

    public List<Account> getAllAccounts() {
        return accountRepository.findAll();
    }

    public Optional<Account> getAccountById(UUID id) {
        return accountRepository.findById(id);
    }

    @Transactional
    public void reconcileReservations() {
        log.info("Starting reservation reconciliation");
        List<Account> accounts = accountRepository.findAll();
        Iterable<TransactionReservation> pending = transactionsRestClient.get()
        .uri("/api/v1/transactions/pending")
        .retrieve()
        .body(new ParameterizedTypeReference<Iterable<TransactionReservation>>() {});

        Map<UUID, BigDecimal> expectedReservations = new HashMap<>();

        for (TransactionReservation tx : pending) {
            expectedReservations.merge(
                tx.getSourceAccountId(),
                tx.getAmount(),
                BigDecimal::add
            );
        }

        for (Account acc: accounts) {
            // if (acc.getReservedAmount().compareTo(BigDecimal.ZERO) > 0) {
            //     acc.setReservedAmount(BigDecimal.ZERO);
            //     accountRepository.save(acc);
            // }
            BigDecimal expected =
            expectedReservations.getOrDefault(acc.getId(), BigDecimal.ZERO);

            if (acc.getReservedAmount().compareTo(expected) != 0) {

                log.warn(
                    "Fixing reservation drift for account {}: {} → {}",
                    acc.getId(),
                    acc.getReservedAmount(),
                    expected
                );

                acc.setReservedAmount(expected);
                accountRepository.save(acc);
            }
        }
        log.info("Reconciliation completed");
    }

    // ============================================================================
    // SYNC FLOW - Called by REST Controller (returns response directly)
    // ============================================================================

    @Transactional
    public AccountReservationResponse reserveAmountSync(AccountReservationRequest request) {
        log.info("Processing SYNC reservation for transaction {}", request.transactionId());
        
        try {
            return doReservation(request.sourceAccountId(), request.amount(), request.transactionId());
        } catch (AccountNotFoundException | InsufficientBalanceException e) {
            log.error("Reservation failed for transaction {}: {}", request.transactionId(), e.getMessage());
            return AccountReservationResponse.failure(
                request.transactionId(),
                request.sourceAccountId(),
                request.amount(),
                e.getMessage()
            );
        }
    }

    @Transactional
    public AccountCommitResponse commitReservationSync(AccountCommitRequest request) {
        log.info("Processing SYNC commit for transaction {}", request.transactionId());
        
        try {
            return doCommit(request.sourceAccountId(), request.destinationAccountId(), 
                          request.amount(), request.transactionId());
        } catch (AccountNotFoundException | InsufficientBalanceException e) {
            log.error("Commit failed for transaction {}: {}", request.transactionId(), e.getMessage());
            return AccountCommitResponse.failure(
                request.transactionId(),
                request.sourceAccountId(),
                request.amount(),
                e.getMessage()
            );
        }
    }

    @Transactional
    public AccountReleaseResponse releaseReservationSync(AccountReleaseRequest request) {
        log.info("Processing SYNC release for transaction {}", request.transactionId());
        
        try {
            return doRelease(request.accountId(), request.amount(), request.transactionId());
        } catch (AccountNotFoundException | InsufficientBalanceException e) {
            log.error("Release failed for transaction {}: {}", request.transactionId(), e.getMessage());
            return AccountReleaseResponse.failure(
                request.transactionId(),
                request.accountId(),
                request.amount(),
                e.getMessage()
            );
        }
    }

    // ============================================================================
    // ASYNC FLOW - Called by Kafka Consumer (publishes event response)
    // ============================================================================

    @Transactional
    public void processReservationAsync(TransactionCreatedEvent event) {
        log.info("Processing ASYNC reservation for transaction {}", event.getTransactionId());
        
        AccountReservationResponse response;
        try {
            response = doReservation(
            event.getSourceAccountId(), 
            event.getAmount(), 
            event.getTransactionId()
        );
        
        log.info("Funds reserved successfully for transaction {}", event.getTransactionId());
            
        } catch (AccountNotFoundException | InsufficientBalanceException e) {
            log.error("Reservation failed for transaction {}: {}", event.getTransactionId(), e.getMessage());
            
            response = AccountReservationResponse.failure(
                event.getTransactionId(),
                event.getSourceAccountId(),
                event.getAmount(),
                e.getMessage()
            );
        } catch (Exception e) {
            log.error("Unexpected error processing reservation for transaction {}: {}", 
                event.getTransactionId(), e.getMessage());
            
            response = AccountReservationResponse.failure(
                event.getTransactionId(),
                event.getSourceAccountId(),
                event.getAmount(),
                "Unexpected error: " + e.getMessage()
            );
        }
        
        accountProducer.publishReservationResponse(response);
    }

    @Transactional
    public void processCommitAsync(TransactionCompletedEvent event) {
        log.info("Processing ASYNC commit for transaction {}", event.transactionId());
        
        AccountCommitResponse response;
        try {
            response = doCommit(
                event.sourceAccountId(),
                event.destinationAccountId(),
                event.amount(),
                event.transactionId()
            );
            
            log.info("Transaction {} committed successfully", event.transactionId());
            
        } catch (AccountNotFoundException | InsufficientBalanceException e) {
            log.error("Commit failed for transaction {}: {}", event.transactionId(), e.getMessage());
            
            response = AccountCommitResponse.failure(
                    event.transactionId(),
                    event.sourceAccountId(),
                    event.amount(),
                    e.getMessage()
                );
        } catch (Exception e) {
            log.error("Unexpected error committing transaction {}: {}", 
                event.transactionId(), e.getMessage());
            
            response = AccountCommitResponse.failure(
                    event.transactionId(),
                    event.sourceAccountId(),
                    event.amount(),
                    "Unexpected error: " + e.getMessage()
                );
        }
        
        accountProducer.publishCommitResponse(response);
    }

    @Transactional
    public void processReleaseAsync(TransactionFailedEvent event) {
        log.info("Processing ASYNC release for transaction {}", event.transactionId());
        
        try {
            doRelease(event.accountId(), event.amount(), event.transactionId());
            log.info("Funds released for transaction {}", event.transactionId());
        } catch (Exception e) {
            log.error("Error releasing funds for transaction {}: {}", 
                event.transactionId(), e.getMessage());
        }
    }

    // ============================================================================
    // Used by both sync and async
    // ============================================================================

    private AccountReservationResponse doReservation(UUID sourceAccountId, BigDecimal amount, UUID transactionId) {
        Account account = accountRepository.findByIdWithLock(sourceAccountId)
                .orElseThrow(() -> new AccountNotFoundException(
                    "Account not found: " + sourceAccountId));

        BigDecimal availableBalance = account.getBalance().subtract(account.getReservedAmount());

        if (availableBalance.compareTo(amount) < 0) {
            throw new InsufficientBalanceException(
                "Insufficient balance. Available: " + availableBalance + ", Requested: " + amount);
        }

        account.setReservedAmount(account.getReservedAmount().add(amount));
        accountRepository.save(account);

        BigDecimal remainingBalance = account.getBalance().subtract(account.getReservedAmount());

        return AccountReservationResponse.success(
            transactionId,
            account.getId(),
            amount,
            remainingBalance
        );
    }

    private AccountCommitResponse doCommit(UUID sourceAccountId, UUID destinationAccountId, 
                                          BigDecimal amount, UUID transactionId) {
        Account sourceAccount = accountRepository.findByIdWithLock(sourceAccountId)
                .orElseThrow(() -> new AccountNotFoundException(
                    "Source account not found: " + sourceAccountId));

        Account destAccount = accountRepository.findByIdWithLock(destinationAccountId)
                .orElseThrow(() -> new AccountNotFoundException(
                    "Destination account not found: " + destinationAccountId));

        if (sourceAccount.getReservedAmount().compareTo(amount) < 0) {
            throw new InsufficientBalanceException(
                "Reserved amount insufficient. Reserved: " + sourceAccount.getReservedAmount() 
                + ", Requested: " + amount);
        }

        // Deduct from source
        sourceAccount.setBalance(sourceAccount.getBalance().subtract(amount));
        sourceAccount.setReservedAmount(sourceAccount.getReservedAmount().subtract(amount));
        accountRepository.save(sourceAccount);

        // Credit to destination
        destAccount.setBalance(destAccount.getBalance().add(amount));
        accountRepository.save(destAccount);

        return AccountCommitResponse.success(
            transactionId,
            sourceAccount.getId(),
            amount,
            sourceAccount.getBalance(),
            destAccount.getId(),
            destAccount.getBalance()
        );
    }

    private AccountReleaseResponse doRelease(UUID accountId, BigDecimal amount, UUID transactionId) {
        Account account = accountRepository.findByIdWithLock(accountId)
                .orElseThrow(() -> new AccountNotFoundException(
                    "Account not found: " + accountId));

        if (account.getReservedAmount().compareTo(amount) < 0) {
            throw new InsufficientBalanceException(
                "Cannot release more than reserved. Reserved: " + account.getReservedAmount() 
                + ", Requested: " + amount);
        }

        account.setReservedAmount(account.getReservedAmount().subtract(amount));
        accountRepository.save(account);

        BigDecimal availableBalance = account.getBalance().subtract(account.getReservedAmount());

        return AccountReleaseResponse.success(
            transactionId,
            account.getId(),
            amount,
            availableBalance
        );
    }
}