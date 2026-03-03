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
    WHERE status IN (
    'PENDING'
    )
    """)
    List<Transaction> findTransactionsWithActiveReservations();

    // how many transactions are not done
    @Query("""
    SELECT COUNT(*) FROM transactions
    WHERE status IN (
    'PENDING'
    )
    """)
    long countActiveReservations();

    // transactions older than 30 seconds
    @Query("""
    SELECT COUNT(*) FROM transactions
    WHERE status NOT IN ('COMPLETED','FAILED')
    AND created_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 SECOND)
    """)
    long countStuckTransactions();
}
