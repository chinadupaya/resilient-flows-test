package com.example.account_service.model.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record AccountReservationResponse(
    UUID transactionId,
    UUID accountId,
    BigDecimal reservedAmount,
    BigDecimal remainingBalance,
    boolean success,
    String message
) {
    // Static factory methods for clean construction in the service and controller
    public static AccountReservationResponse success(
            UUID transactionId, UUID accountId,
            BigDecimal reservedAmount, BigDecimal remainingBalance) {
        return new AccountReservationResponse(
            transactionId, accountId, reservedAmount, remainingBalance, true, "Reservation successful"
        );
    }

    public static AccountReservationResponse failed(UUID transactionId, String message) {
        return new AccountReservationResponse(
            transactionId, null, null, null, false, message
        );
    }
}