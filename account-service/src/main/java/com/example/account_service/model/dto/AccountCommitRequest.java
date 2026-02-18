package com.example.account_service.model.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record AccountCommitRequest(
    UUID transactionId,
    UUID sourceAccountId,
    UUID destinationAccountId,
    BigDecimal amount
) {

    
}