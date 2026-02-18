package com.example.transaction_service.model.dto;

import java.math.BigDecimal;
import java.util.UUID;

public class CreateTransactionRequest {

    private UUID sourceAccountId;
    private UUID destinationAccountId;
    private BigDecimal amount;

    public UUID getSourceAccountId() { return sourceAccountId; }
    public void setSourceAccountId(UUID sourceAccountId) { this.sourceAccountId = sourceAccountId; }
    public UUID getDestinationAccountId() { return destinationAccountId; }
    public void setDestinationAccountId(UUID destinationAccountId) { this.destinationAccountId = destinationAccountId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
}