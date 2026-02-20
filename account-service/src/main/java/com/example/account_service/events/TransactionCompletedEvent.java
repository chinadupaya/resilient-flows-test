package com.example.account_service.events;
import java.math.BigDecimal;
import java.util.UUID;

public record TransactionCompletedEvent(
        String eventType,
        UUID transactionId,
        UUID sourceAccountId,
        UUID destinationAccountId,
        BigDecimal amount
) implements TransactionEvent {

    // Convenience constructor — NO eventType parameter
    public TransactionCompletedEvent(
            UUID transactionId,
            UUID sourceAccountId,
            UUID destinationAccountId,
            BigDecimal amount
    ) {
        this(
                "TRANSACTION_COMPLETED",
                transactionId,
                sourceAccountId,
                destinationAccountId,
                amount
        );
    }
}
