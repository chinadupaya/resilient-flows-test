package com.example.transaction_service.model.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record AccountCommitResponse(
    UUID transactionId,
    UUID sourceAccountId,
    BigDecimal committedAmount,
    BigDecimal sourceNewBalance,
    UUID destAccountId,
    BigDecimal destNewBalance,
    boolean success,
    String message
) {
    public static AccountCommitResponse success(
            UUID transactionId, UUID sourceAccountId,
            BigDecimal committedAmount, BigDecimal sourceNewBalance,
            UUID destAccountId, BigDecimal destNewBalance) {
        return new AccountCommitResponse(
            transactionId, sourceAccountId, committedAmount, sourceNewBalance, 
            destAccountId, destNewBalance, true, "Commit successful"
        );
    }

    public static AccountCommitResponse failure(
            UUID transactionId, UUID sourceAccountId, 
            BigDecimal amount, String message) {
        return new AccountCommitResponse(
            transactionId, sourceAccountId, amount, null, 
            null, null, false, message
        );
    }
}