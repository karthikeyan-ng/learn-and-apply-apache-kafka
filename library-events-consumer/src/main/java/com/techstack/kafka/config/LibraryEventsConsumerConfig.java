package com.techstack.kafka.config;

import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

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
        return  factory;
    }

}
