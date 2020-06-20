package com.techstack.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<>();

        /**
         * By enabling this Concurrency Consumer Container will run in a 3 different threads.
         * Each Thread will point to a partition.
         */
        factory.setConcurrency(3);

        configurer.configure(factory, kafkaConsumerFactory);

        //Default AckMode is BATCH
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        /**
         * Adding Custom Error handling mechanism
         * By default Kafka Consumer Error handling mechanism would work. However, if you want to
         * override these functionality and execute certain logic after error got thrown then below
         * implementation would work.
         */
        factory.setErrorHandler(((thrownException, data) -> {
            log.info("Exception in consumerConfig is {} and the record is {}", thrownException.getMessage(), data);
            //persist

            // DO SOME BUSINESS LOGIC AFTER ERROR GOT THROWN
        }));

        /**
         * Retry Mechanism
         */
        factory.setRetryTemplate(retryTemplate());

        return  factory;
    }

    private RetryTemplate retryTemplate() {

        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000); //Back Off time is 1 second, before applying retry

        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy()); //step1
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy); //step2
        return  retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {

        /**
         * Scenario 1: With the below given configuration, any exception occurs during the
         * execution logic, retry mechanisms would start.
         *
         * However, if my data itself is wrong and because of that application would throw
         * an exception then it is not a proper retry.
         *
         * When to execute retry? If you got any specific exception or error then only you have
         * to call the retry. Refer Scenario2
         */
        //SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        //simpleRetryPolicy.setMaxAttempts(3); //Retry 3 times

        /**
         * Scenario 2 Impl (Custom Retry logic)
         */
        Map<Class<? extends Throwable>, Boolean> exceptionsMap = new HashMap<>();
        exceptionsMap.put(IllegalArgumentException.class, false);
        exceptionsMap.put(RecoverableDataAccessException.class, true);

        var simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionsMap,true);
        return simpleRetryPolicy;
    }
}
