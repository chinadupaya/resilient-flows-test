package com.example.transaction_service.workflow;

import com.example.transaction_service.model.dto.CreateTransactionRequest;
import com.example.transaction_service.service.TransactionService;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.springboot.RestateService;

@RestateService
public class TransactionWorkflow {

    private final TransactionService transactionService;

    public TransactionWorkflow(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @Handler
    public void run(CreateTransactionRequest request) {
        transactionService.createTransaction(request);
    }
}