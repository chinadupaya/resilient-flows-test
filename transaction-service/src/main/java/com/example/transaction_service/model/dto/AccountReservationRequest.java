package com.example.transaction_service.model.dto;

import java.math.BigDecimal;
import java.util.UUID;

public class AccountReservationRequest {

    private UUID transactionId;
    private UUID sourceAccountId;
    private UUID destinationAccountId;
    private BigDecimal amount;

    public AccountReservationRequest(UUID transactionId, UUID sourceAccountId,
                                     UUID destinationAccountId, BigDecimal amount) {
        this.transactionId = transactionId;
        this.sourceAccountId = sourceAccountId;
        this.destinationAccountId = destinationAccountId;
        this.amount = amount;
    }

    public UUID getTransactionId() { return transactionId; }
    public UUID getSourceAccountId() { return sourceAccountId; }
    public UUID getDestinationAccountId() { return destinationAccountId; }
    public BigDecimal getAmount() { return amount; }
}