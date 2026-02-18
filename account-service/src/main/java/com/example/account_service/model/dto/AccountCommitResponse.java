package com.example.account_service.model.dto;

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
            BigDecimal committedAmount, BigDecimal newBalance,
            UUID destAccountId,BigDecimal destNewBalance) {
        return new AccountCommitResponse(
            transactionId, sourceAccountId, committedAmount, newBalance, destAccountId, destNewBalance, true, "Commit successful"
        );
    }

    public static AccountCommitResponse failed(UUID transactionId, String message) {
        return new AccountCommitResponse(
            transactionId, null, null, null, null, null, false, message
        );
    }
}