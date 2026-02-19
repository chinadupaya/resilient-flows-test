package com.example.account_service.events;

import java.math.BigDecimal;
import java.util.UUID;

import com.example.account_service.model.dto.AccountReleaseResponse;

public record AccountReleaseEvent(
    String eventType,
    UUID transactionId,
    UUID accountId,
    BigDecimal releasedAmount,
    BigDecimal availableBalance,
    boolean success,
    String message
) implements AccountEvent {
    public AccountReleaseEvent(
        UUID transactionId,
        UUID accountId,
        BigDecimal releasedAmount,
        BigDecimal availableBalance,
        boolean success,
        String message
    ) {
        this("RELEASE_RESPONSE", transactionId, accountId, releasedAmount, availableBalance, success, message);
    }

    public static AccountReleaseEvent fromResponse(AccountReleaseResponse response) {
        return new AccountReleaseEvent(
            response.transactionId(),
            response.accountId(),
            response.releasedAmount(),
            response.availableBalance(),
            response.success(),
            response.message()

        );
    }
}