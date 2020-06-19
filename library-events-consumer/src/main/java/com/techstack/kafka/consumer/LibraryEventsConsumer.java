package com.techstack.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsConsumer {

    /**
     * Key Point here:
     * 1. Kafka container have 1..N number of messages.
     * 2. However when we use @KafkaListener will poll messages one by one. Hence we are
     * processing single ConsumerRecord (message)
     */
    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer,String> consumerRecord) {

        log.info("ConsumerRecord : {} ", consumerRecord );

    }
}
