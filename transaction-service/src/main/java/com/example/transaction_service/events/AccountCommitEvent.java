package com.example.transaction_service.events;

import java.math.BigDecimal;
import java.util.UUID;

import com.example.transaction_service.model.dto.AccountCommitResponse;

public record AccountCommitEvent(
    String eventType,
    UUID transactionId,
    UUID sourceAccountId,
    BigDecimal committedAmount,
    BigDecimal sourceNewBalance,
    UUID destAccountId,
    BigDecimal destNewBalance,
    boolean success,
    String message
) implements AccountEvent {

    // Convenience constructor
    public AccountCommitEvent(
        UUID transactionId,
        UUID sourceAccountId,
        BigDecimal committedAmount,
        BigDecimal sourceNewBalance,
        UUID destAccountId,
        BigDecimal destNewBalance,
        boolean success,
        String message
    ) {
        this(
            "COMMIT_RESPONSE",
            transactionId,
            sourceAccountId,
            committedAmount,
            sourceNewBalance,
            destAccountId,
            destNewBalance,
            success,
            message
        );
    }

    public static AccountCommitEvent fromResponse(AccountCommitResponse response) {
        return new AccountCommitEvent(
            response.transactionId(),
            response.sourceAccountId(),
            response.committedAmount(),
            response.sourceNewBalance(),
            response.destAccountId(),
            response.destNewBalance(),
            response.success(),
            response.message()
        );
    }
}
