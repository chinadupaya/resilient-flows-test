package com.example.account_service.model.dto;


import java.util.UUID;
import java.math.BigDecimal;

public record AccountRefundResponse(
    UUID transactionId,
    UUID sourceAccountId,
    UUID destinationAccountId,
    BigDecimal amount,
    String status,
    String message
) {
    public static AccountRefundResponse success(
        UUID txId, UUID src, UUID dst, BigDecimal amount) {
        return new AccountRefundResponse(txId, src, dst, amount, "SUCCESS", null);
    }

    public static AccountRefundResponse failure(
        UUID txId, UUID src, UUID dst, BigDecimal amount, String msg) {
        return new AccountRefundResponse(txId, src, dst, amount, "FAILED", msg);
    }
}