package com.example.account_service.model.dto;
import java.math.BigDecimal;
import java.util.UUID;

public class TransactionReservation {

    private UUID transactionId;
    private UUID sourceAccountId;
    private BigDecimal amount;

    public TransactionReservation() {}

    public TransactionReservation(UUID transactionId, UUID sourceAccountId, BigDecimal amount) {
        this.transactionId = transactionId;
        this.sourceAccountId = sourceAccountId;
        this.amount = amount;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(UUID transactionId) {
        this.transactionId = transactionId;
    }

    public UUID getSourceAccountId() {
        return sourceAccountId;
    }

    public void setSourceAccountId(UUID sourceAccountId) {
        this.sourceAccountId = sourceAccountId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }
}