package com.example.transaction_service.repository;

import com.example.transaction_service.model.Transaction;
import com.google.cloud.spring.data.spanner.repository.SpannerRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TransactionRepository extends SpannerRepository<Transaction, String> {
}
