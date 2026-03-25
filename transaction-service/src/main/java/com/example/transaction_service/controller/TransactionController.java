package com.example.transaction_service.controller;


import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.example.transaction_service.model.Transaction;
import com.example.transaction_service.model.dto.CreateTransactionRequest;
import com.example.transaction_service.model.dto.TransactionReservation;
import com.example.transaction_service.service.TransactionService;
import com.example.transaction_service.model.TransactionStatus;
import com.example.transaction_service.model.TransactionType;

import com.example.transaction_service.workflow.TransactionWorkflowClient;
import dev.restate.client.Client;


@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {
    private final TransactionService transactionService;
    private final Client restateClient;

    public TransactionController(TransactionService transactionService,
        Client restateClient
    ) {
        this.transactionService = transactionService;
        this.restateClient = restateClient;
    }

    @GetMapping()
    public ResponseEntity<Iterable<Transaction>> getAllTransactions() {
        return ResponseEntity.ok(transactionService.getAllTransactions());
    }

    @GetMapping("/pending")
    public ResponseEntity<Iterable<TransactionReservation>> getPendingTransactions() {
        return ResponseEntity.ok(transactionService.getPendingTransactions());
    }
    

    @PostMapping
    public ResponseEntity<?> createTransaction(@RequestBody CreateTransactionRequest request) {
        // Validate type field is present
        if (request.getType() == null || request.getType().isEmpty()) {
            return ResponseEntity.badRequest().body("Field 'type' is required (sync or async)");
        }
        
        // Normalize to uppercase for comparison
        String type = request.getType().toUpperCase();

        if(TransactionType.SYNC.name().equals(type)) {
            
            Transaction transaction = transactionService.createTransaction(request);
            // Transaction transaction = TransactionWorkflowClient.fromClient(restateClient).run(request);
            if (TransactionStatus.COMPLETED.name().equals(transaction.getStatus())) {
                return ResponseEntity.status(HttpStatus.CREATED).body(transaction);
            } else {
                return ResponseEntity.status(HttpStatus.CONFLICT).body(transaction);
            }
        } else if (TransactionType.SYNCV2.name().equals(type)) {
            System.out.println("logging transactionworkflow");
            Transaction transaction = TransactionWorkflowClient.fromClient(restateClient).run(request);
            if (TransactionStatus.COMPLETED.name().equals(transaction.getStatus())) {
                return ResponseEntity.status(HttpStatus.CREATED).body(transaction);
            } else {
                return ResponseEntity.status(HttpStatus.CONFLICT).body(transaction);
            }
        } else if (TransactionType.ASYNC.name().equals(type)) {
            // Asynchronous flow - publish to Kafka and return accepted
            Transaction transaction = transactionService.createTransactioAsync(request);

            return ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(transaction); 
        }
        return ResponseEntity.badRequest()
                .body("Invalid type. Must be 'sync' or 'async'");

    }
    
}