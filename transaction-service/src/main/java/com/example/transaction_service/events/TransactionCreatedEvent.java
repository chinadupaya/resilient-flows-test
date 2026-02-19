package com.example.transaction_service.events;

import java.math.BigDecimal;
import java.util.UUID;

public record TransactionCreatedEvent (
    String eventType,
    UUID transactionId,
    UUID sourceAccountId,
    UUID destinationAccountId,
    BigDecimal amount
    
) implements TransactionEvent{

    public TransactionCreatedEvent(
        UUID transactionId,
        UUID sourceAccountId,
        UUID destinationAccountId,
        BigDecimal amount
    ) {
        this(
            "TRANSACTION_CREATED", 
            transactionId, 
            sourceAccountId, 
            destinationAccountId, 
            amount
        );

    }


    public UUID getTransactionId() { return transactionId; }
    // public void setTransactionId(UUID transactionId) { this.transactionId = transactionId; }
    public UUID getSourceAccountId() { return sourceAccountId; }
    // public void setSourceAccountId(UUID sourceAccountId) { this.sourceAccountId = sourceAccountId; }
    public UUID getDestinationAccountId() { return destinationAccountId; }
    // public void setDestinationAccountId(UUID destinationAccountId) { this.destinationAccountId = destinationAccountId; }
    public BigDecimal getAmount() { return amount; }
    // public void setAmount(BigDecimal amount) { this.amount = amount; }
}