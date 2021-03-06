package com.techstack.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.techstack.kafka.domain.LibraryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
public class LibraryEventProducer {

    private static final String TOPIC_NAME = "library-events";

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

    public ListenableFuture<SendResult<Integer, String>> sendLibraryEvent_Approach2(final LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        /**
         * This is an asynchronous call. Which is going to return immediately as soon as this KafkaTemplate method
         * call returned
         *
         * By using KafkaTemplate send() using another overloaded method which is accepting {@link ProducerRecord}.
         * So, it will take information from the ProducerRecord.
         */
        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, TOPIC_NAME);
        ListenableFuture<SendResult<Integer, String>> listenableFuture =  kafkaTemplate.send(producerRecord);

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

        return listenableFuture;
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {

        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));

        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous(final LibraryEvent libraryEvent)
            throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        /**
         * When you use get(), you are going to wait until the future returned successfully.
         * Basically it's going to wait until the future is resolved to on success or on failure
         */
        SendResult<Integer, String> sendResult;
        try {
            //Without Timeout
            //sendResult = kafkaTemplate.sendDefault(key, value).get();

            //With TimeOut: It would wait max 1 seconds to get the response from the Kafka, else timeout
            sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            log.error("InterruptedException/ExecutionException sending the message and the exception is {}", e.getMessage());
            throw  e;
        } catch (Exception e) {
            log.error("Exception sending the message and the exception is {}", e.getMessage());
            throw e;
        }

        return sendResult;
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
