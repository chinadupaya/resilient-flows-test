package com.example.account_service.model.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record AccountReleaseRequest(
    UUID accountId,
    UUID transactionId,
    BigDecimal amount
) {}