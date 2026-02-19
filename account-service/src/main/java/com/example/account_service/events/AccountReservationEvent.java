package com.example.account_service.events;

import java.math.BigDecimal;
import java.util.UUID;
import com.example.account_service.model.dto.AccountReservationResponse;

public record AccountReservationEvent(
    String eventType,
    UUID transactionId,
    UUID accountId,
    BigDecimal reservedAmount,
    BigDecimal remainingBalance,
    String status,
    String message
) implements AccountEvent {
    
    public AccountReservationEvent(
        UUID transactionId,
        UUID accountId,
        BigDecimal reservedAmount,
        BigDecimal remainingBalance,
        String status,
        String message
    ) {
        this("RESERVATION_RESPONSE", transactionId, accountId, reservedAmount, remainingBalance, status, message);
    }
    
    public static AccountReservationEvent fromResponse(AccountReservationResponse response) {
        return new AccountReservationEvent(
            response.transactionId(),
            response.accountId(),
            response.reservedAmount(),
            response.remainingBalance(),
            response.success() ? "SUCCESS" : "FAILED",
            response.message()
        );
    }
}