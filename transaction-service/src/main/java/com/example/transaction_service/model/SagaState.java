package com.example.transaction_service.model;

public enum SagaState {
    STARTED,
    ACCOUNT_RESERVATION_REQUESTED,
    ACCOUNT_RESERVATION_SUCCESS,
    ACCOUNT_COMMIT_REQUESTED,
    COMPLETED,
    COMPENSATED
}
