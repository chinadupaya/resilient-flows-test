package com.example.transaction_service.events;

import java.math.BigDecimal;
import java.util.UUID;

public class TransactionEvent {
    private UUID transactionId;
    private UUID sourceAccountId;
    private UUID destinationAccountId;
    private BigDecimal amount;

    public TransactionEvent() {}

    public TransactionEvent(UUID transactionId, UUID sourceAccountId, 
                           UUID destinationAccountId, BigDecimal amount) {
        this.transactionId = transactionId;
        this.sourceAccountId = sourceAccountId;
        this.destinationAccountId = destinationAccountId;
        this.amount = amount;
    }

    public UUID getTransactionId() { return transactionId; }
    public void setTransactionId(UUID transactionId) { this.transactionId = transactionId; }
    public UUID getSourceAccountId() { return sourceAccountId; }
    public void setSourceAccountId(UUID sourceAccountId) { this.sourceAccountId = sourceAccountId; }
    public UUID getDestinationAccountId() { return destinationAccountId; }
    public void setDestinationAccountId(UUID destinationAccountId) { this.destinationAccountId = destinationAccountId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
}