package com.example.transaction_service;

import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
    classes = TransactionServiceApplication.class,
    properties = {
        "spring.main.lazy-initialization=true"
    }
)
class TransactionServiceApplicationTests {

    @Test
    void contextLoads() {
    }
}
