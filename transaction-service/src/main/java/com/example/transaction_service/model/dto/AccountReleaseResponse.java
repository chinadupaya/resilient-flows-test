package com.example.transaction_service.model.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record AccountReleaseResponse(
    UUID transactionId,
    UUID accountId,
    BigDecimal releasedAmount,
    BigDecimal availableBalance,
    boolean success,
    String message
) {
    public static AccountReleaseResponse success(
            UUID transactionId, UUID accountId,
            BigDecimal releasedAmount, BigDecimal availableBalance) {
        return new AccountReleaseResponse(
            transactionId, accountId, releasedAmount, availableBalance, true, "Release successful"
        );
    }

    public static AccountReleaseResponse failure(
        UUID transactionId, UUID accountId,
        BigDecimal amount, String message) {
        return new AccountReleaseResponse(
            transactionId, accountId, amount, null, false, message
        );
    }
}