package com.example.account_service.events;
import java.math.BigDecimal;
import java.util.UUID;
import java.time.Instant;

public record TransactionCompletedEvent(
        String eventType,
        UUID transactionId,
        UUID sourceAccountId,
        UUID destinationAccountId,
        BigDecimal amount,
        Instant updatedAt
) implements TransactionEvent {

    // Convenience constructor — NO eventType parameter
    public TransactionCompletedEvent(
            UUID transactionId,
            UUID sourceAccountId,
            UUID destinationAccountId,
            BigDecimal amount,
            Instant updatedAt
    ) {
        this(
                "TRANSACTION_COMPLETED",
                transactionId,
                sourceAccountId,
                destinationAccountId,
                amount,
                updatedAt
        );
    }
}
