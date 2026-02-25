package com.example.account_service.events;

import java.math.BigDecimal;
import java.util.UUID;

public record TransactionFailedEvent(
        String eventType,
        UUID transactionId,
        UUID accountId,
        BigDecimal amount,
        String reason
) implements TransactionEvent{
    public TransactionFailedEvent(
        UUID transactionId,
        UUID accountId,
        BigDecimal amount,
        String reason
    ) {
        this(
            "TRANSACTION_FAILED", 
            transactionId, 
            accountId,
            amount,
            reason
        );
    }

}
