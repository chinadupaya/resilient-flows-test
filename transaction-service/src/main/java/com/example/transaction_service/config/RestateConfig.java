package com.example.transaction_service.config;

import dev.restate.client.Client;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RestateConfig {

    @Bean
    public Client restateClient() {
        return Client.connect("http://localhost:8080"); // Restate server ingress port
    }
}