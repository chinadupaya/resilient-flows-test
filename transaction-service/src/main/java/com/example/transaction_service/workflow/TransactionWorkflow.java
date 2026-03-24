package com.example.transaction_service.workflow;

import com.example.transaction_service.model.Transaction;
import com.example.transaction_service.model.dto.AccountReservationRequest;
import com.example.transaction_service.model.dto.CreateTransactionRequest;
import com.example.transaction_service.service.TransactionService;

import dev.restate.sdk.Context;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.springboot.RestateService;
import dev.restate.sdk.common.TerminalException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID; 

@RestateService
public class TransactionWorkflow {

    private final TransactionService transactionService;

    public TransactionWorkflow(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @Handler
    public Transaction run(Context ctx, CreateTransactionRequest request) throws TerminalException {

        List<Runnable> compensations = new ArrayList<>();

        Transaction transaction = ctx.run("initiate txn", Transaction.class,
                () -> transactionService.createInitialTransaction(request));

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

            compensations.add(() ->
                    ctx.run("cancel reservation request",
                            () -> transactionService.cancelReservationRequested(transaction.getId()))
            );

            // STEP 2 — Reserve funds
            boolean reserved = ctx.run("reserve funds", Boolean.class,
                    () -> transactionService.reserveFunds(reservationRequest));

            if (!reserved) {
                throw new TerminalException("Reservation failed");
            }

            ctx.run("mark reservation success",
                    () -> transactionService.markReservationSuccess(transaction.getId()));

            // STEP 3 — Compensation AFTER successful reservation
            compensations.add(() ->
                    ctx.run("release funds",
                            () -> transactionService.compensateReservation(
                                    transaction.getId(), reservationRequest))
            );

            // STEP 4 — Compliance
            boolean compliant = ctx.run("compliance check", Boolean.class,
                    () -> transactionService.checkCompliance(transaction.getId()));

            if (!compliant) {
                throw new TerminalException("Compliance failed");
            }

            // STEP 5 — Commit requested
            ctx.run("mark commit requested",
                    () -> transactionService.markCommitRequested(transaction.getId()));

            compensations.add(() ->
                    ctx.run("cancel commit request",
                            () -> transactionService.compensateCommitRequested(
                                    transaction.getId(), reservationRequest))
            );

            // STEP 6 — Commit funds
            boolean committed = ctx.run("commit funds", Boolean.class,
                    () -> transactionService.commitFunds(reservationRequest));

            if (!committed) {
                throw new TerminalException("Commit failed");
            }

            compensations.add(() ->
                    ctx.run("cancel commit",
                            () -> transactionService.compensateCommit(transaction.getId()))
            );

            // STEP 7 — Complete transaction
            return ctx.run("complete transaction", Transaction.class,
                    () -> transactionService.completeTransaction(transaction.getId()));

        } catch (TerminalException e) {

            // Execute compensations in reverse order
            for (int i = compensations.size() - 1; i >= 0; i--) {
                compensations.get(i).run();
            }

            ctx.run("mark failed",
                    () -> transactionService.failTransaction(transaction.getId(), e.getMessage()));

            throw e;
        }
    }
}

// @RestateService
// public class TransactionWorkflow {

//     private final TransactionService transactionService;

//     public TransactionWorkflow(TransactionService transactionService) {
//         this.transactionService = transactionService;
//     }

//     @Handler
//     public Transaction run(Context ctx, CreateTransactionRequest request) {
        
//         Transaction transaction = ctx.run("initiate txn", Transaction.class, () ->
//                 transactionService.createInitialTransaction(request)
//         );

//         AccountReservationRequest reservationRequest = new AccountReservationRequest(
//                 UUID.fromString(transaction.getId()),
//                 request.getSourceAccountId(),
//                 request.getDestinationAccountId(),
//                 transaction.getAmount()
//         );

//         try {
//             ctx.run("txn marked reservation requested", () -> 
//                 transactionService.markReservationRequested(transaction.getId())
//             );

//              boolean reserved = ctx.run("request reservation", Boolean.class, () ->
//                     transactionService.reserveFunds(reservationRequest)
//             );

//             if (!reserved) {
//                 return ctx.run("failed to reserve", Transaction.class, () ->
//                         transactionService.failTransaction(transaction.getId(), "Account reservation failed")
//                 );
//             }

//             ctx.run("reservation marked success", () -> {
//                 transactionService.markReservationSuccess(transaction.getId());
//             });

//             boolean compliant = ctx.run("check compliance", Boolean.class, () ->
//                     transactionService.checkCompliance(transaction.getId())
//             );

//             if (!compliant) {
//                 ctx.run("release funds", () -> {
//                     transactionService.releaseFunds(transaction.getId(), reservationRequest);
//                 });

//                 return ctx.run("failed to release", Transaction.class, () ->
//                         transactionService.failTransaction(transaction.getId(), "Compliance checks failed")
//                 );
//             }

//             ctx.run("request commit", () -> {
//                 transactionService.markCommitRequested(transaction.getId());
//             });

//             boolean committed = ctx.run("txn committed",Boolean.class, () ->
//                     transactionService.commitFunds(reservationRequest)
//             );

//             if (!committed) {
//                 ctx.run("release funds",() -> {
//                     transactionService.releaseFunds(transaction.getId(), reservationRequest);
//                 });

//                 return ctx.run("failed txn - account not committed", Transaction.class, () ->
//                         transactionService.failTransaction(transaction.getId(), "Account commit failed")
//                 );
//             }

//             return ctx.run("mark completed", Transaction.class, () ->
//                     transactionService.completeTransaction(transaction.getId())
//             );

//         } catch (Exception e) {
//             ctx.run(Void.class, () -> {
//                 transactionService.releaseFunds(transaction.getId(), reservationRequest);
//                 return null;
//             });

//             return ctx.run(Transaction.class, () ->
//                     transactionService.failTransaction(transaction.getId(), e.getMessage())
//             );
//         }
        
//         // return ctx.run("Create Transaction", Transaction.class, () -> transactionService.createTransaction(request));
//     }
// }