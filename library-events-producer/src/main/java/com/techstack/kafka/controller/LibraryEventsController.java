package com.techstack.kafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.techstack.kafka.domain.LibraryEvent;
import com.techstack.kafka.producer.LibraryEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsController {

    private final LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/library-event")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent)
            throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        log.info("before sendLibraryEvent");

        //Approach1: Asynchronous call
        /**
         * Here, you will see the asynchronous behaviour of your message.
         * Because, the message before reaches the Kafka broker, it would send a success message and
         * controller would return the HTTPStatus as CREATED
         *
         * TIP: The message producer behaviour will be run in a different thread
         */
        //libraryEventProducer.sendLibraryEvent(libraryEvent);

        //Approach2: Synchronous call
        /**
         * In this synchronous approach, after SendResult invoked, and then after sendLibraryEvent will be printed
         */
        SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
        log.info("SendResult is {}", sendResult.toString());

        log.info("after sendLibraryEvent");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
