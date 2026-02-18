package com.example.account_service.model.dto;

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

    public static AccountReleaseResponse failed(UUID transactionId, String message) {
        return new AccountReleaseResponse(
            transactionId, null, null, null, false, message
        );
    }
}