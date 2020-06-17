package com.techstack.kafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.techstack.kafka.domain.LibraryEvent;
import com.techstack.kafka.producer.LibraryEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsController {

    private final LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/library-event")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        /**
         * Here, you will see the asynchronous behaviour of your message.
         * Because, the message before reaches the Kafka broker, it would send a success message and
         * controller would return the HTTPStatus as CREATED
         *
         * TIP: The message producer behaviour will be run in a different thread
         */

        log.info("before sendLibraryEvent");

        libraryEventProducer.sendLibraryEvent(libraryEvent);

        log.info("after sendLibraryEvent");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
