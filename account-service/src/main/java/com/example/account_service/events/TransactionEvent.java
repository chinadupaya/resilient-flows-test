package com.example.account_service.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "eventType"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TransactionCreatedEvent.class, name = "TRANSACTION_CREATED"),
    @JsonSubTypes.Type(value = TransactionFailedEvent.class, name = "TRANSACTION_FAILED"),
    @JsonSubTypes.Type(value = TransactionCompletedEvent.class, name = "TRANSACTION_COMPLETED")
})

public sealed interface TransactionEvent permits TransactionCreatedEvent, TransactionFailedEvent, TransactionCompletedEvent {
    String eventType();
}