package com.example.transaction_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import dev.restate.sdk.springboot.EnableRestate;


@SpringBootApplication
@EnableRestate
// @Import(RestateEndpointConfiguration.class)
public class TransactionServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(TransactionServiceApplication.class, args);
	}

}
