package com.techstack.kafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.techstack.kafka.domain.LibraryEvent;
import com.techstack.kafka.producer.LibraryEventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RestController
public class LibraryEventsController {

    private final LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/library-event")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

        libraryEventProducer.sendLibraryEvent(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
