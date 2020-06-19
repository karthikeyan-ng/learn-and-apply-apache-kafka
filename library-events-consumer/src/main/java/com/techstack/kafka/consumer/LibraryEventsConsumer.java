package com.techstack.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.techstack.kafka.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class LibraryEventsConsumer {

    private final LibraryEventsService libraryEventsService;

    /**
     * Key Point here:
     * 1. Kafka container have 1..N number of messages.
     * 2. However when we use @KafkaListener will poll messages one by one. Hence we are
     * processing single ConsumerRecord (message)
     * 3. @KafkaListener annotation uses the  ConcurrentMessageListener
     * 4. With ConcurrentMessageListener we can spin up multiple instances of the same Kafka message
     * 5. If your application running on Cloud / Kubernaties environment, this option (ConcurrentMessageListener) is not necessary
     */
    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {

        log.info("ConsumerRecord : {} ", consumerRecord );
        libraryEventsService.processLibraryEvent(consumerRecord);

    }
}
