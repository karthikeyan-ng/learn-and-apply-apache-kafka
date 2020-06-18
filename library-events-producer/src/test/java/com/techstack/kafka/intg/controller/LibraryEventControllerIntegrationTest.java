package com.techstack.kafka.intg.controller;

import com.techstack.kafka.domain.Book;
import com.techstack.kafka.domain.LibraryEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class LibraryEventControllerIntegrationTest {

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Test
    @DisplayName("Post a Library Event and Verify")
    void postLibraryEvent() {

        //Given
        Book book = Book.builder().bookId(123).bookAuthor("Karthi").bookName("TDD").build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                                                .libraryEventId(null)
                                                .book(book)
                                                .build();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        //When
        ResponseEntity<LibraryEvent> responseEntity =
                testRestTemplate.exchange("/v1/library-event", HttpMethod.POST, request, LibraryEvent.class);

        //Then
        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
    }
}
