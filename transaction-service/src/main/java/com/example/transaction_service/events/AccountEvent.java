package com.example.transaction_service.events;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "eventType"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = AccountReservationEvent.class, name = "RESERVATION_RESPONSE"),
    @JsonSubTypes.Type(value = AccountCommitEvent.class, name = "COMMIT_RESPONSE"),
    @JsonSubTypes.Type(value = AccountReleaseEvent.class, name = "RELEASE_RESPONSE")
})
public sealed interface AccountEvent permits AccountReservationEvent, AccountCommitEvent, AccountReleaseEvent {
    String eventType();
}