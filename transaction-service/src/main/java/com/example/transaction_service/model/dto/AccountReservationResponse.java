package com.example.transaction_service.model.dto;

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
    public static AccountReservationResponse success(
            UUID transactionId, UUID accountId,
            BigDecimal reservedAmount, BigDecimal remainingBalance) {
        return new AccountReservationResponse(
            transactionId, accountId, reservedAmount, remainingBalance, true, "Reservation successful"
        );
    }

    public static AccountReservationResponse failure(
            UUID transactionId, UUID accountId,
            BigDecimal amount, String message) {
        return new AccountReservationResponse(
            transactionId, accountId, amount, null, false, message
        );
    }
}