package com.example.transaction_service.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;

import com.example.transaction_service.model.Transaction;
import com.example.transaction_service.model.dto.CreateTransactionRequest;
import com.example.transaction_service.model.dto.TransactionEvent;
import com.example.transaction_service.service.TransactionProducer;
import com.example.transaction_service.service.TransactionService;
import com.example.transaction_service.model.TransactionStatus;
import com.example.transaction_service.model.TransactionType;



@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {
    private final TransactionService transactionService;
    private final TransactionProducer transactionProducer;

    public TransactionController(TransactionService transactionService, 
        TransactionProducer transactionProducer
    ) {
        this.transactionService = transactionService;
        this.transactionProducer = transactionProducer;
    }

    @GetMapping()
    public ResponseEntity<Iterable<Transaction>> getAllTransactions() {
        return ResponseEntity.ok(transactionService.getAllTransactions());
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
            
            if (TransactionStatus.COMPLETED.name().equals(transaction.getStatus())) {
                return ResponseEntity.status(HttpStatus.CREATED).body(transaction);
            } else {
                return ResponseEntity.status(HttpStatus.CONFLICT).body(transaction);
            }
        } else if (TransactionType.ASYNC.name().equals(type)) {
            // Asynchronous flow - publish to Kafka and return accepted
            Transaction transaction = transactionService.createTransactionInitial(request);

            TransactionEvent event = new TransactionEvent(
                    UUID.fromString(transaction.getId()),
                    request.getSourceAccountId(),
                    request.getDestinationAccountId(),
                    request.getAmount()
            );

            transactionProducer.publishTransactionEvent(event);

            return ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(transaction); 
        }
        return ResponseEntity.badRequest()
                .body("Invalid type. Must be 'sync' or 'async'");

    }
    
}