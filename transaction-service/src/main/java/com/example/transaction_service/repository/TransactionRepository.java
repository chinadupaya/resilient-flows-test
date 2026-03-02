package com.example.transaction_service.repository;

import com.example.transaction_service.model.Transaction;
import com.google.cloud.spring.data.spanner.repository.SpannerRepository;
import com.google.cloud.spring.data.spanner.repository.query.Query;

import java.util.List;

import org.springframework.stereotype.Repository;

@Repository
public interface TransactionRepository extends SpannerRepository<Transaction, String> {

    @Query("""
    SELECT * FROM transactions
    WHERE saga_state IN (
    'ACCOUNT_RESERVATION_REQUESTED',
    'ACCOUNT_RESERVATION_SUCCESS',
    'ACCOUNT_COMMIT_REQUESTED',
    'COMPENSATION_REQUESTED'
    )
    """)
    List<Transaction> findTransactionsWithActiveReservations();

    // how many transactions are currently holding funds
    @Query("""
    SELECT COUNT(*) FROM transactions
    WHERE saga_state IN (
    'ACCOUNT_RESERVATION_REQUESTED',
    'ACCOUNT_RESERVATION_SUCCESS',
    'ACCOUNT_COMMIT_REQUESTED'
    )
    """)
    long countActiveReservations();

    // transactions older than 30 seconds
    @Query("""
    SELECT COUNT(*) FROM transactions
    WHERE saga_state NOT IN ('COMPLETED','FAILED')
    AND created_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 SECOND)
    """)
    long countStuckTransactions();
}
