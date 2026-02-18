-- Create accounts table
CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_holder_name VARCHAR(255) NOT NULL,
    balance NUMERIC(19, 2) NOT NULL DEFAULT 0.00,
    reserved_amount NUMERIC(19, 2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Dummy data
INSERT INTO accounts (account_holder_name, balance, reserved_amount)
VALUES
    ('Alice Johnson', 1500.00, 200.00),
    ('Bob Smith', 3200.50, 0.00),
    ('Carol White', 750.75, 100.00),
    ('David Brown', 10000.00, 500.00),
    ('Eve Davis', 250.00, 0.00);