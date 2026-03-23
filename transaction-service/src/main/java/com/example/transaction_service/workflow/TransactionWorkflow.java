package com.example.transaction_service.workflow;

import com.example.transaction_service.model.Transaction;
import com.example.transaction_service.model.dto.AccountReservationRequest;
import com.example.transaction_service.model.dto.CreateTransactionRequest;
import com.example.transaction_service.service.TransactionService;

import dev.restate.sdk.Context;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.springboot.RestateService;

import java.time.Instant;
import java.util.UUID; 

@RestateService
public class TransactionWorkflow {

    private final TransactionService transactionService;

    public TransactionWorkflow(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @Handler
    public Transaction run(Context ctx, CreateTransactionRequest request) {
        
        Transaction transaction = ctx.run("initiate txn", Transaction.class, () ->
                transactionService.createInitialTransaction(request)
        );

        AccountReservationRequest reservationRequest = new AccountReservationRequest(
                UUID.fromString(transaction.getId()),
                request.getSourceAccountId(),
                request.getDestinationAccountId(),
                transaction.getAmount()
        );

        try {
            ctx.run("txn marked reservation requested", () -> 
                transactionService.markReservationRequested(transaction.getId())
            );

             boolean reserved = ctx.run("request reservation", Boolean.class, () ->
                    transactionService.reserveFunds(reservationRequest)
            );

            if (!reserved) {
                return ctx.run("failed to reserve", Transaction.class, () ->
                        transactionService.failTransaction(transaction.getId(), "Account reservation failed")
                );
            }

            ctx.run("reservation marked success", () -> {
                transactionService.markReservationSuccess(transaction.getId());
            });

            boolean compliant = ctx.run("check compliance", Boolean.class, () ->
                    transactionService.checkCompliance(transaction.getId())
            );

            if (!compliant) {
                ctx.run("release funds", () -> {
                    transactionService.releaseFunds(transaction.getId(), reservationRequest);
                });

                return ctx.run("failed to release", Transaction.class, () ->
                        transactionService.failTransaction(transaction.getId(), "Compliance checks failed")
                );
            }

            ctx.run("request commit", () -> {
                transactionService.markCommitRequested(transaction.getId());
            });

            boolean committed = ctx.run("txn committed",Boolean.class, () ->
                    transactionService.commitFunds(reservationRequest)
            );

            if (!committed) {
                ctx.run("release funds",() -> {
                    transactionService.releaseFunds(transaction.getId(), reservationRequest);
                });

                return ctx.run("failed txn - account not committed", Transaction.class, () ->
                        transactionService.failTransaction(transaction.getId(), "Account commit failed")
                );
            }

            return ctx.run("mark completed", Transaction.class, () ->
                    transactionService.completeTransaction(transaction.getId())
            );

        } catch (Exception e) {
            ctx.run(Void.class, () -> {
                transactionService.releaseFunds(transaction.getId(), reservationRequest);
                return null;
            });

            return ctx.run(Transaction.class, () ->
                    transactionService.failTransaction(transaction.getId(), e.getMessage())
            );
        }
        
        // return ctx.run("Create Transaction", Transaction.class, () -> transactionService.createTransaction(request));
    }
}