package com.techstack.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.techstack.kafka.domain.LibraryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@Component
@RequiredArgsConstructor
public class LibraryEventProducer {

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void sendLibraryEvent(final LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        /**
         * Here we didn't mention where to send the topic in Kafka.
         * Because, we have configured 'default-topic' information in application.yml file.
         * Hence, Kafka template know where to send this message.
         *
         * This is an asynchronous call. Which is going to return immediately as soon as this KafkaTemplate method
         * call returned
         */
        ListenableFuture<SendResult<Integer, String>> listenableFuture =  kafkaTemplate.sendDefault(key, value);

        /**
         * ListenableFuture has a option of add  call back
         */
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

            /**
             * This method get invoked if the message that published is ended up with failure
             * @param throwable
             */
            @Override
            public void onFailure(Throwable throwable) {
                handleFailure(key, value, throwable);
            }

            /**
             * This method gets invoked if the publish is successful
             *
             * @param result
             */
            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });

    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending the message and the exception is {}", ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error in OnFailure {}", throwable.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message send successfully for the key: {} and the value is {}, partition info is {}", key, value, result.getRecordMetadata().partition());
    }

}
