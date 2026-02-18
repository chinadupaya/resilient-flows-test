# Step-by-Step Saga Flows - Happy Path

## 🔄 Asynchronous Saga (Kafka-based) - Happy Path

### Initial State
```
Account A: balance=$10,000, reserved=$0
Account B: balance=$5,000, reserved=$0
```

### User Request
```bash
POST http://localhost:8080/api/transactions
{
  "sourceAccountId": "account-001",
  "destinationAccountId": "account-002", 
  "amount": 100.00
}
```

---

### STEP 1: Transaction Service Receives Request

**Location:** `TransactionService.createTransaction()`

**Actions:**
1. Generate new transaction ID: `"txn-abc123"`
2. Create transaction record in database:
   ```
   Transaction {
     id: "txn-abc123"
     sourceAccountId: "account-001"
     destinationAccountId: "account-002"
     amount: 100.00
     status: PENDING
     sagaState: ACCOUNT_VALIDATION_REQUESTED
     createdAt: 2024-02-11 10:00:00
   }
   ```
3. **DB COMMIT** ✅ Transaction saved to `transactions` table

**Database State:**
- **transactions table:** 1 new row (PENDING)

---

### STEP 2: Transaction Service Publishes to Kafka

**Location:** `TransactionService.publishTransactionCreateRequest()`

**Actions:**
1. Build event:
   ```java
   TransactionCreateRequestEvent {
     transactionId: "txn-abc123"
     sourceAccountId: "account-001"
     destinationAccountId: "account-002"
     amount: 100.00
     timestamp: 2024-02-11 10:00:00
   }
   ```
2. Publish to Kafka topic: `transaction.create.request`
3. **KAFKA COMMIT** ✅ Event available in Kafka

**Response to User:**
```json
HTTP 200 OK
{
  "transactionId": "txn-abc123",
  "status": "PENDING",
  "message": "Transaction initiated successfully"
}
```

**Current State:**
- Transaction DB: status=PENDING, sagaState=ACCOUNT_VALIDATION_REQUESTED
- Account A: balance=$10,000, reserved=$0 (unchanged)
- Account B: balance=$5,000, reserved=$0 (unchanged)
- Kafka: Event waiting to be consumed

---

### STEP 3: Account Service Receives Kafka Event

**Location:** `AccountService.handleTransactionCreateRequest()`

**Consumer:** Kafka listener on topic `transaction.create.request`

**Actions:**
1. Kafka delivers event to Account Service consumer
2. Deserialize event to `TransactionCreateRequestEvent`
3. Begin processing...

---

### STEP 4: Account Service Validates Accounts

**Location:** `AccountService.handleTransactionCreateRequest()` (continued)

**Actions:**
1. **Find source account:**
   ```sql
   SELECT * FROM accounts WHERE id = 'account-001'
   ```
   Result: Account found ✅

2. **Check balance:**
   ```
   Account A balance: $10,000
   Requested amount: $100
   Validation: $10,000 >= $100 ✅
   ```

3. **Find destination account:**
   ```sql
   SELECT * FROM accounts WHERE id = 'account-002'
   ```
   Result: Account found ✅

---

### STEP 5: Account Service Reserves Amount

**Location:** `AccountService.handleTransactionCreateRequest()` (continued)

**Actions:**
1. Update source account:
   ```java
   sourceAccount.setBalance($10,000 - $100)      // = $9,900
   sourceAccount.setReservedAmount($0 + $100)    // = $100
   sourceAccount.setUpdatedAt(now)
   ```

2. **DB COMMIT** ✅
   ```sql
   UPDATE accounts 
   SET balance = 9900.00, 
       reserved_amount = 100.00,
       updated_at = '2024-02-11 10:00:01'
   WHERE id = 'account-001'
   ```

**Current State:**
- Account A: balance=$9,900, reserved=$100 ✅
- Account B: balance=$5,000, reserved=$0 (unchanged)

**🔍 CDC Captures:** Account A balance change (9900) and reserved amount change (100)

---

### STEP 6: Account Service Publishes Success Event

**Location:** `AccountService.publishAccountValidationSuccess()`

**Actions:**
1. Build event:
   ```java
   AccountValidationSuccessEvent {
     transactionId: "txn-abc123"
     sourceAccountId: "account-001"
     destinationAccountId: "account-002"
     amount: 100.00
     timestamp: 2024-02-11 10:00:01
   }
   ```

2. Publish to Kafka topic: `account.validation.success`
3. **KAFKA COMMIT** ✅

**Current State:**
- Account A: balance=$9,900, reserved=$100
- Kafka: New event waiting for Transaction Service

---

### STEP 7: Transaction Service Receives Success Event

**Location:** `TransactionService.handleAccountValidationSuccess()`

**Consumer:** Kafka listener on topic `account.validation.success`

**Actions:**
1. Kafka delivers event to Transaction Service
2. Find transaction:
   ```sql
   SELECT * FROM transactions WHERE id = 'txn-abc123'
   ```

3. Update saga state:
   ```java
   transaction.setSagaState(ACCOUNT_VALIDATION_SUCCESS)
   transaction.setSagaState(TRANSACTION_PROCESSING)
   ```

---

### STEP 8: Transaction Service Processes Transaction

**Location:** `TransactionService.processTransaction()`

**Actions:**
1. Run business logic:
   - Fraud detection check ✅
   - Compliance check ✅
   - Risk assessment ✅
   - All checks passed ✅

2. Result: `processingSuccess = true`

---

### STEP 9: Transaction Service Completes Transaction

**Location:** `TransactionService.handleAccountValidationSuccess()` (continued)

**Actions:**
1. Update transaction status:
   ```java
   transaction.setStatus(COMPLETED)
   transaction.setSagaState(COMPLETED)
   transaction.setUpdatedAt(now)
   ```

2. **DB COMMIT** ✅
   ```sql
   UPDATE transactions 
   SET status = 'COMPLETED',
       saga_state = 'COMPLETED',
       updated_at = '2024-02-11 10:00:02'
   WHERE id = 'txn-abc123'
   ```

**🔍 CDC Captures:** Transaction status change to COMPLETED

---

### STEP 10: Transaction Service Publishes Completion Event

**Location:** `TransactionService.publishTransactionCompleted()`

**Actions:**
1. Build event:
   ```java
   TransactionCompletedEvent {
     transactionId: "txn-abc123"
     sourceAccountId: "account-001"
     destinationAccountId: "account-002"
     amount: 100.00
     timestamp: 2024-02-11 10:00:02
   }
   ```

2. Publish to Kafka topic: `transaction.completed`
3. **KAFKA COMMIT** ✅

---

### STEP 11: Account Service Receives Completion Event

**Location:** `AccountService.handleTransactionCompleted()`

**Consumer:** Kafka listener on topic `transaction.completed`

**Actions:**
1. Kafka delivers event to Account Service
2. Find both accounts:
   ```sql
   SELECT * FROM accounts WHERE id = 'account-001'
   SELECT * FROM accounts WHERE id = 'account-002'
   ```

---

### STEP 12: Account Service Commits Transfer (FINAL STEP)

**Location:** `AccountService.handleTransactionCompleted()` (continued)

**Actions:**
1. Update source account (release reservation):
   ```java
   sourceAccount.setReservedAmount($100 - $100)   // = $0
   sourceAccount.setUpdatedAt(now)
   ```

2. Update destination account (credit funds):
   ```java
   destAccount.setBalance($5,000 + $100)          // = $5,100
   destAccount.setUpdatedAt(now)
   ```

3. **DB COMMIT** ✅
   ```sql
   UPDATE accounts 
   SET reserved_amount = 0.00,
       updated_at = '2024-02-11 10:00:03'
   WHERE id = 'account-001';
   
   UPDATE accounts 
   SET balance = 5100.00,
       updated_at = '2024-02-11 10:00:03'
   WHERE id = 'account-002';
   ```

**🔍 CDC Captures:** 
- Account A reserved amount change (0)
- Account B balance change (5100)

---

### STEP 13: Account Service Publishes Confirmation

**Location:** `AccountService.publishAccountsUpdated()`

**Actions:**
1. Build event:
   ```java
   AccountsUpdatedEvent {
     transactionId: "txn-abc123"
     sourceAccountId: "account-001"
     destinationAccountId: "account-002"
     amount: 100.00
     timestamp: 2024-02-11 10:00:03
   }
   ```

2. Publish to Kafka topic: `account.updated`
3. **KAFKA COMMIT** ✅

---

### ✅ FINAL STATE - Transaction Complete!

```
Account A: 
  balance = $9,900 (was $10,000, sent $100)
  reserved = $0 (released)

Account B:
  balance = $5,100 (was $5,000, received $100)
  reserved = $0

Transaction:
  id = "txn-abc123"
  status = COMPLETED
  sagaState = COMPLETED
```

**Total Time:** ~3-5 seconds (asynchronous processing)

**Events Published:**
1. `transaction.create.request` → Account Service
2. `account.validation.success` → Transaction Service
3. `transaction.completed` → Account Service
4. `account.updated` → (audit/monitoring)

**Database Changes Captured by CDC:**
1. Transaction created (PENDING)
2. Account A: balance reduced, reservation added
3. Transaction updated (COMPLETED)
4. Account A: reservation released
5. Account B: balance increased

---
---

