package com.example.account_service.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransactionFailedEvent(
        String eventType,
        UUID transactionId,
        UUID accountId,
        BigDecimal amount,
        String reason,
        Instant failedAt
) implements TransactionEvent{
    public TransactionFailedEvent(
        UUID transactionId,
        UUID accountId,
        BigDecimal amount,
        String reason,
        Instant failedAt
    ) {
        this(
            "TRANSACTION_FAILED", 
            transactionId, 
            accountId,
            amount,
            reason, 
            failedAt
        );
    }

}
