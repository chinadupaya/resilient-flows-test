package com.example.transaction_service.workflow;

import com.example.transaction_service.model.Transaction;
import com.example.transaction_service.model.dto.AccountReservationRequest;
import com.example.transaction_service.model.dto.CreateTransactionRequest;
import com.example.transaction_service.service.TransactionService;

import dev.restate.sdk.Context;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.springboot.RestateService;
import dev.restate.sdk.common.TerminalException;

import java.util.UUID; 

@RestateService
public class TransactionWorkflow {

    private final TransactionService transactionService;

    public TransactionWorkflow(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @Handler
    public Transaction run(Context ctx, CreateTransactionRequest request) throws TerminalException {

        Transaction transaction = ctx.run("initiate txn", Transaction.class,
                () -> transactionService.createInitialTransaction(request));

        chaosPoint("AFTER_INIT");

        AccountReservationRequest reservationRequest = new AccountReservationRequest(
                UUID.fromString(transaction.getId()),
                request.getSourceAccountId(),
                request.getDestinationAccountId(),
                transaction.getAmount()
        );

        try {
            // STEP 1 — Reservation requested
            ctx.run("mark reservation requested",
                    () -> transactionService.markReservationRequested(transaction.getId()));

            // STEP 2 — Reserve funds
            boolean reserved = ctx.run("reserve funds", Boolean.class,
                    () -> transactionService.reserveFunds(reservationRequest));

            chaosPoint("AFTER_RESERVE");

            if (!reserved) {
                throw new TerminalException("Reservation failed");
            }

            ctx.run("mark reservation success",
                    () -> transactionService.markReservationSuccess(transaction.getId()));

            // STEP 3 — Compliance
            boolean compliant = ctx.run("compliance check", Boolean.class,
                    () -> transactionService.checkCompliance(transaction.getId()));

            chaosPoint("AFTER_COMPLIANCE");

            if (!compliant) {
                throw new TerminalException("Compliance failed");
            }

            // STEP 4 — Commit requested
            ctx.run("mark commit requested",
                    () -> transactionService.markCommitRequested(transaction.getId()));

            // STEP 5 — Commit funds
            boolean committed = ctx.run("commit funds", Boolean.class,
                    () -> transactionService.commitFunds(reservationRequest));

            chaosPoint("AFTER_COMMIT");

            if (!committed) {
                throw new TerminalException("Commit failed");
            }

            // STEP 6 — Complete transaction
            return ctx.run("complete transaction", Transaction.class,
                    () -> transactionService.completeTransaction(transaction.getId()));

        } catch (TerminalException e) {

            chaosPoint("BEFORE_COMPENSATION");

            ctx.run("compensate transaction",
                    () -> transactionService.compensateTransaction(
                            transaction.getId(), reservationRequest));

            ctx.run("mark failed",
                    () -> transactionService.failTransaction(
                            transaction.getId(), e.getMessage()));

            throw e;
        }
    }

    private void chaosPoint(String name) {
        String chaos = System.getenv("CHAOS_POINT");
        if (chaos != null && chaos.equals(name)) {
            System.err.println("CHAOS: crashing at " + name);
            Runtime.getRuntime().halt(1);
        }
    }
}
