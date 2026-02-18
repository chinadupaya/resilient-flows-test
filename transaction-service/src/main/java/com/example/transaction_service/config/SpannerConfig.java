// package com.example.transaction_service.config;

// import com.google.api.gax.core.CredentialsProvider;
// import com.google.api.gax.core.NoCredentialsProvider;
// import org.springframework.context.annotation.Bean;
// import org.springframework.context.annotation.Configuration;
// import org.springframework.context.annotation.Profile;

// @Configuration
// public class SpannerConfig {

//     @Bean
//     @Profile("!prod")  // Use NoCredentials for local/dev, not in production
//     public CredentialsProvider credentialsProvider() {
//         return NoCredentialsProvider.create();
//     }
// }