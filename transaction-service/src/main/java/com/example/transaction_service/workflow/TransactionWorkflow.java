package com.example.transaction_service.workflow;


import com.example.transaction_service.model.Transaction;
import com.example.transaction_service.model.dto.CreateTransactionRequest;
import com.example.transaction_service.service.TransactionService;

import dev.restate.sdk.annotation.Handler;
// import dev.restate.sdk.annotation.Service;
import dev.restate.sdk.springboot.RestateService;
import dev.restate.sdk.Context;

@RestateService
public class TransactionWorkflow {

    private final TransactionService transactionService;

    public TransactionWorkflow(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @Handler
    public void run(Context ctx, CreateTransactionRequest request) {
        Transaction tx = ctx.run(
            Transaction.class,
            () -> transactionService.createTransaction(request)
        );
    }

}


// import dev.restate.sdk.annotation.Handler;
// import dev.restate.sdk.annotation.Service;
// import dev.restate.sdk.Context;

// @Service
// public class TransactionWorkflow {

//     @Handler
//     public String start(Context ctx, String input) {
//         return "ok-" + input;
//     }
// }