package com.example.transaction_service.workflow;

import com.example.transaction_service.model.Transaction;
import com.example.transaction_service.model.dto.CreateTransactionRequest;
import com.example.transaction_service.service.TransactionService;

import dev.restate.sdk.Context;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.springboot.RestateService;

@RestateService
public class TransactionWorkflow {

    private final TransactionService transactionService;

    public TransactionWorkflow(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @Handler
    public Transaction run(Context ctx, CreateTransactionRequest request) {
        return ctx.run("Create Transaction", Transaction.class, () -> transactionService.createTransaction(request));
    }
}