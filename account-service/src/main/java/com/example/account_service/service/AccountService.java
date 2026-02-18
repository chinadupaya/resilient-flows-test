package com.example.account_service.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.account_service.model.Account;
import com.example.account_service.model.dto.AccountCommitRequest;
import com.example.account_service.model.dto.AccountCommitResponse;
import com.example.account_service.model.dto.AccountReservationRequest;
import com.example.account_service.model.dto.AccountReservationResponse;
import com.example.account_service.model.dto.AccountReleaseRequest;
import com.example.account_service.model.dto.AccountReleaseResponse;
import com.example.account_service.repository.AccountRepository;
import com.example.account_service.exception.AccountNotFoundException;
import com.example.account_service.exception.InsufficientBalanceException;

@Service
public class AccountService {

    private final AccountRepository accountRepository;

    // Constructor injection — preferred over @Autowired
    public AccountService(AccountRepository accountRepository) {
        this.accountRepository = accountRepository;
    }

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
    public AccountReservationResponse reserveAmount(AccountReservationRequest request) {
        Account account = accountRepository.findByIdWithLock(request.sourceAccountId())
                .orElseThrow(() -> new AccountNotFoundException(
                    "Account not found: " + request.sourceAccountId()));

        // Calculate available balance (total balance minus already reserved)
        BigDecimal availableBalance = account.getBalance()
                .subtract(account.getReservedAmount());

        if (availableBalance.compareTo(request.amount()) < 0) {
            throw new InsufficientBalanceException(
                "Insufficient balance. Available: " + availableBalance
                + ", Requested: " + request.amount());
        }

        // Add to reserved amount — does not deduct balance until settlement
        account.setReservedAmount(account.getReservedAmount().add(request.amount()));
        accountRepository.save(account);

        BigDecimal remainingBalance = account.getBalance()
                .subtract(account.getReservedAmount());

        return AccountReservationResponse.success(
            request.transactionId(),
            account.getId(),
            request.amount(),
            remainingBalance
        );
    }

    @Transactional
    public AccountCommitResponse commitReservation(AccountCommitRequest request) {
        // Fetch account with lock to prevent concurrent modifications
        Account account = accountRepository.findByIdWithLock(request.sourceAccountId())
                .orElseThrow(() -> new AccountNotFoundException(
                    "Source Account not found: " + request.sourceAccountId()));

        Account destAccount = accountRepository.findByIdWithLock(request.destinationAccountId())
                .orElseThrow(() -> new AccountNotFoundException("Destination Account not found: " + request.sourceAccountId()));

        // Check that reserved amount covers what we're trying to commit
        if (account.getReservedAmount().compareTo(request.amount()) < 0) {
            throw new InsufficientBalanceException(
                "Reserved amount insufficient. Reserved: " + account.getReservedAmount()
                + ", Requested to commit: " + request.amount());
        }

        // Deduct from both balance and reserved amount — this is the actual settlement
        account.setBalance(account.getBalance().subtract(request.amount()));
        account.setReservedAmount(account.getReservedAmount().subtract(request.amount()));
        accountRepository.save(account);

        destAccount.setBalance(destAccount.getBalance().add(request.amount()));
        accountRepository.save(destAccount);

        return AccountCommitResponse.success(
            request.transactionId(),
            account.getId(),
            request.amount(),
            account.getBalance(),
            destAccount.getId(),
            destAccount.getBalance()
        );
    }

    @Transactional
    public AccountReleaseResponse releaseReservation(AccountReleaseRequest request) {
        // Fetch account with lock to prevent concurrent modifications
        Account account = accountRepository.findByIdWithLock(request.accountId())
                .orElseThrow(() -> new AccountNotFoundException(
                    "Account not found: " + request.accountId()));

        // Check that there is enough reserved amount to roll back
        if (account.getReservedAmount().compareTo(request.amount()) < 0) {
            throw new InsufficientBalanceException(
                "Cannot release more than reserved. Reserved: " + account.getReservedAmount()
                + ", Requested release: " + request.amount());
        }

        // Release the reservation — balance is untouched, only reservedAmount decreases
        account.setReservedAmount(account.getReservedAmount().subtract(request.amount()));
        accountRepository.save(account);

        // Available balance is balance minus what's still reserved after release
        BigDecimal availableBalance = account.getBalance()
                .subtract(account.getReservedAmount());

        return AccountReleaseResponse.success(
            request.transactionId(),
            account.getId(),
            request.amount(),
            availableBalance
        );
    }

}