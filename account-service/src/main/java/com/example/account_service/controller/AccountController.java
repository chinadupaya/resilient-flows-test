package com.example.account_service.controller;

import java.util.List;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


import com.example.account_service.service.AccountService;

import lombok.extern.slf4j.Slf4j;

import com.example.account_service.model.Account;

import com.example.account_service.model.dto.AccountReleaseResponse;
import com.example.account_service.model.dto.AccountReservationResponse;
import com.example.account_service.model.dto.AccountCommitResponse;
import com.example.account_service.model.dto.AccountReleaseRequest;
import com.example.account_service.model.dto.AccountReservationRequest;
import com.example.account_service.model.dto.AccountCommitRequest;
import com.example.account_service.exception.InsufficientBalanceException;
import com.example.account_service.exception.AccountNotFoundException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;


@Slf4j
@RestController
@RequestMapping("/api/v1/accounts")
public class AccountController {

    private final AccountService accountService;

    // Constructor injection instead of @Autowired
    public AccountController(AccountService accountService) {
        this.accountService = accountService;
    }

    @PostMapping
    public ResponseEntity<Account> createAccount(@RequestBody CreateAccountRequest request) {
        Account created = accountService.createAccount(request.accountHolderName());
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @GetMapping
    public ResponseEntity<List<Account>> getAllAccounts() {
        List<Account> accounts = accountService.getAllAccounts();
        return ResponseEntity.ok(accounts);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Account> getAccountById(@PathVariable UUID id) {
        return accountService.getAccountById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }


    @PostMapping("/reserve")
    public ResponseEntity<AccountReservationResponse> reserveAmount(
            @RequestBody AccountReservationRequest request) {
        log.info("Received reservation request for transaction: {}", request.transactionId());
        
        try {
            AccountReservationResponse response = accountService.reserveAmountSync(request);
            return ResponseEntity.ok(response);
        } catch (InsufficientBalanceException e) {
            log.warn("Insufficient balance: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(AccountReservationResponse.failure(request.transactionId(), request.sourceAccountId(), request.amount(), e.getMessage()));
        } catch (AccountNotFoundException e) {
            log.error("Account not found: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(AccountReservationResponse.failure(request.transactionId(), request.sourceAccountId(), request.amount(), e.getMessage()));
        } catch (Exception e) {
            log.error("Error reserving amount", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(AccountReservationResponse.failure(request.transactionId(), request.sourceAccountId(), request.amount(), e.getMessage()));
        }
    }

    @PostMapping("/commit")
    public ResponseEntity<AccountCommitResponse> commitReservation(
            @RequestBody AccountCommitRequest request) {
        log.info("Received commit request for transaction: {}", request.transactionId());

        try {
            AccountCommitResponse response = accountService.commitReservationSync(request);
            return ResponseEntity.ok(response);
        } catch (InsufficientBalanceException e) {
            log.warn("Insufficient reserved amount: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(AccountCommitResponse.failure(request.transactionId(), request.sourceAccountId(), request.amount(), e.getMessage()));
        } catch (AccountNotFoundException e) {
            log.error("Account not found: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(AccountCommitResponse.failure(request.transactionId(), request.sourceAccountId(), request.amount(), e.getMessage()));
        } catch (Exception e) {
            log.error("Error committing reservation", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(AccountCommitResponse.failure(request.transactionId(), request.sourceAccountId(), request.amount(), e.getMessage()));
        }
    }

    @PostMapping("/release")
    public ResponseEntity<AccountReleaseResponse> releaseReservation(
            @RequestBody AccountReleaseRequest request) {
        log.info("Received release request for transaction: {}", request.transactionId());

        try {
            AccountReleaseResponse response = accountService.releaseReservationSync(request);
            return ResponseEntity.ok(response);
        } catch (InsufficientBalanceException e) {
            log.warn("Cannot release, insufficient reserved amount: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(AccountReleaseResponse.failure(request.transactionId(), request.accountId(), request.amount(), e.getMessage()));
        } catch (AccountNotFoundException e) {
            log.error("Account not found: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(AccountReleaseResponse.failure(request.transactionId(), request.accountId(), request.amount(), e.getMessage()));
        } catch (Exception e) {
            log.error("Error rolling back reservation", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(AccountReleaseResponse.failure(request.transactionId(), request.accountId(), request.amount(), e.getMessage()));
        }
    }

    @PostMapping("/reconcile")
    public void reconcileReservations() {
        accountService.reconcileReservations();
        return;
    }
    

    // Simple request DTO as a record — keeps request data off the Account model
    public record CreateAccountRequest(String accountHolderName) {}
}