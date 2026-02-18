
## 🔗 Synchronous Saga (REST-based) - Happy Path

### Initial State
```
Account A: balance=$10,000, reserved=$0
Account B: balance=$5,000, reserved=$0
```

### User Request
```bash
POST http://localhost:8082/api/transactions
{
  "sourceAccountId": "account-001",
  "destinationAccountId": "account-002",
  "amount": 100.00
}
```

---

### STEP 1: Transaction Service Receives Request

**Location:** `TransactionController.createTransaction()`

**Actions:**
1. Request forwarded to `TransactionOrchestrator.executeTransactionSaga()`
2. Method begins **synchronous** orchestration

---

### STEP 2: Create Transaction Record

**Location:** `TransactionOrchestrator.createTransaction()`

**Actions:**
1. Generate transaction ID: `"txn-xyz789"`
2. Create transaction in database:
   ```java
   Transaction {
     id: "txn-xyz789"
     sourceAccountId: "account-001"
     destinationAccountId: "account-002"
     amount: 100.00
     status: PENDING
     sagaState: STARTED
     createdAt: 2024-02-11 10:00:00
   }
   ```

3. **DB COMMIT** ✅ Transaction saved

**Current State:**
- Transaction DB: status=PENDING, sagaState=STARTED

**🔍 CDC Captures:** New transaction with STARTED state

---

### STEP 3: Update Saga State to Reservation Requested

**Location:** `TransactionOrchestrator.executeTransactionSaga()`

**Actions:**
1. Update transaction:
   ```java
   transaction.setSagaState(ACCOUNT_RESERVATION_REQUESTED)
   ```

2. **DB COMMIT** ✅

**🔍 CDC Captures:** Saga state change to ACCOUNT_RESERVATION_REQUESTED

---

### STEP 4: Call Account Service - Reserve Amount

**Location:** `TransactionOrchestrator.callAccountReservation()`

**Actions:**
1. Build REST request:
   ```java
   AccountReservationRequest {
     transactionId: "txn-xyz789"
     sourceAccountId: "account-001"
     destinationAccountId: "account-002"
     amount: 100.00
   }
   ```

2. Make synchronous HTTP call:
   ```java
   POST http://localhost:8081/api/accounts/reserve
   Content-Type: application/json
   ```

3. **⏳ WAITING** for Account Service response...

---

### STEP 5: Account Service Receives REST Call

**Location:** `AccountController.reserveAmount()` → `AccountService.reserveAmount()`

**Actions:**
1. HTTP request received
2. Extract request body
3. Begin processing...

---

### STEP 6: Account Service Validates Accounts

**Location:** `AccountService.reserveAmount()`

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

### STEP 7: Account Service Reserves Amount

**Location:** `AccountService.reserveAmount()` (continued)

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
       updated_at = '2024-02-11 10:00:00.100'
   WHERE id = 'account-001'
   ```

**Current State:**
- Account A: balance=$9,900, reserved=$100 ✅

**🔍 CDC Captures:** Account A balance and reservation changes

---

### STEP 8: Account Service Returns Success Response

**Location:** `AccountController.reserveAmount()`

**Actions:**
1. Build response:
   ```java
   AccountReservationResponse {
     transactionId: "txn-xyz789"
     success: true
     message: "Amount reserved successfully"
     accountId: "account-001"
     currentBalance: 9900.00
     reservedAmount: 100.00
   }
   ```

2. Return HTTP response:
   ```
   HTTP 200 OK
   Content-Type: application/json
   ```

3. **⏳ Response travels back** to Transaction Service...

---

### STEP 9: Transaction Service Receives Reservation Response

**Location:** `TransactionOrchestrator.callAccountReservation()` returns

**Actions:**
1. Receive HTTP response
2. Check response status:
   ```java
   if (!reservationResponse.isSuccess()) {
     // Handle failure - NOT this path
   }
   ```

3. Response is successful ✅
4. Log: "Account reservation successful"

---

### STEP 10: Update Saga State to Reservation Success

**Location:** `TransactionOrchestrator.executeTransactionSaga()`

**Actions:**
1. Update transaction:
   ```java
   transaction.setSagaState(ACCOUNT_RESERVATION_SUCCESS)
   ```

2. **DB COMMIT** ✅

**🔍 CDC Captures:** Saga state change to ACCOUNT_RESERVATION_SUCCESS

---

### STEP 11: Process Transaction (Business Logic)

**Location:** `TransactionOrchestrator.executeTransactionSaga()`

**Actions:**
1. Update saga state:
   ```java
   transaction.setSagaState(TRANSACTION_PROCESSING)
   ```

2. **DB COMMIT** ✅

3. Call `processTransaction()`:
   - Run fraud detection ✅
   - Run compliance checks ✅
   - Run risk assessment ✅
   - All passed ✅

4. Result: `processingSuccess = true`

**🔍 CDC Captures:** Saga state change to TRANSACTION_PROCESSING

---

### STEP 12: Update Saga State to Commit Requested

**Location:** `TransactionOrchestrator.executeTransactionSaga()`

**Actions:**
1. Update transaction:
   ```java
   transaction.setSagaState(ACCOUNT_COMMIT_REQUESTED)
   ```

2. **DB COMMIT** ✅

**🔍 CDC Captures:** Saga state change to ACCOUNT_COMMIT_REQUESTED

---

### STEP 13: Call Account Service - Commit Transaction

**Location:** `TransactionOrchestrator.callAccountCommit()`

**Actions:**
1. Build REST request:
   ```java
   AccountCommitRequest {
     transactionId: "txn-xyz789"
     sourceAccountId: "account-001"
     destinationAccountId: "account-002"
     amount: 100.00
   }
   ```

2. Make synchronous HTTP call:
   ```java
   POST http://localhost:8081/api/accounts/commit
   Content-Type: application/json
   ```

3. **⏳ WAITING** for Account Service response...

---

### STEP 14: Account Service Receives Commit Request

**Location:** `AccountController.commitReservation()` → `AccountService.commitReservation()`

**Actions:**
1. HTTP request received
2. Find both accounts:
   ```sql
   SELECT * FROM accounts WHERE id = 'account-001'
   SELECT * FROM accounts WHERE id = 'account-002'
   ```

---

### STEP 15: Account Service Commits Transfer

**Location:** `AccountService.commitReservation()`

**Actions:**
1. Update source account (release reservation):
   ```java
   sourceAccount.setReservedAmount($100 - $100)  // = $0
   sourceAccount.setUpdatedAt(now)
   ```

2. Update destination account (credit funds):
   ```java
   destAccount.setBalance($5,000 + $100)         // = $5,100
   destAccount.setUpdatedAt(now)
   ```

3. **DB COMMIT** ✅ (Both accounts in single transaction)
   ```sql
   UPDATE accounts 
   SET reserved_amount = 0.00,
       updated_at = '2024-02-11 10:00:00.250'
   WHERE id = 'account-001';
   
   UPDATE accounts 
   SET balance = 5100.00,
       updated_at = '2024-02-11 10:00:00.250'
   WHERE id = 'account-002';
   ```

**Current State:**
- Account A: balance=$9,900, reserved=$0 ✅
- Account B: balance=$5,100, reserved=$0 ✅

**🔍 CDC Captures:** 
- Account A reservation released (0)
- Account B balance increased (5100)

---

### STEP 16: Account Service Returns Commit Response

**Location:** `AccountController.commitReservation()`

**Actions:**
1. Build response:
   ```java
   AccountCommitResponse {
     transactionId: "txn-xyz789"
     success: true
     message: "Transaction committed successfully"
     sourceBalance: 9900.00
     destinationBalance: 5100.00
   }
   ```

2. Return HTTP response:
   ```
   HTTP 200 OK
   ```

3. **⏳ Response travels back** to Transaction Service...

---

### STEP 17: Transaction Service Receives Commit Response

**Location:** `TransactionOrchestrator.callAccountCommit()` returns

**Actions:**
1. Receive HTTP response
2. Check response status:
   ```java
   if (!commitResponse.isSuccess()) {
     // Handle failure - NOT this path
   }
   ```

3. Response is successful ✅
4. Log: "Transaction saga completed successfully"

---

### STEP 18: Mark Transaction as Complete (FINAL STEP)

**Location:** `TransactionOrchestrator.executeTransactionSaga()`

**Actions:**
1. Update transaction:
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
       updated_at = '2024-02-11 10:00:00.300'
   WHERE id = 'txn-xyz789'
   ```

3. Return transaction to controller

**🔍 CDC Captures:** Transaction status change to COMPLETED

---

### STEP 19: Return Response to User

**Location:** `TransactionController.createTransaction()`

**Actions:**
1. Build response:
   ```java
   TransactionResponse {
     transactionId: "txn-xyz789"
     status: COMPLETED
     message: "Transaction completed successfully"
   }
   ```

2. Return to user:
   ```
   HTTP 201 Created
   Content-Type: application/json
   ```

3. User receives response immediately! ✅

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
  id = "txn-xyz789"
  status = COMPLETED
  sagaState = COMPLETED
```

**Total Time:** ~100-300ms (synchronous, immediate response)

**REST Calls Made:**
1. POST /api/accounts/reserve (Transaction → Account)
2. POST /api/accounts/commit (Transaction → Account)

**Database Changes Captured by CDC:**
1. Transaction created (STARTED)
2. Saga state: ACCOUNT_RESERVATION_REQUESTED
3. Account A: balance reduced, reservation added
4. Saga state: ACCOUNT_RESERVATION_SUCCESS
5. Saga state: TRANSACTION_PROCESSING
6. Saga state: ACCOUNT_COMMIT_REQUESTED
7. Account A: reservation released
8. Account B: balance increased
9. Transaction updated (COMPLETED)

---