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
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)

/**
 * During IntegrationTest if your test don't want to call the actual Kafka broker, then
 * you can use @EmbeddedKafka with other attributes
 */
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)

/**
 * If you refer {@link org.springframework.kafka.test.EmbeddedKafkaBroker} class which contains
 * a property called "spring.embedded.kafka.brokers" to run Kafka broker in a test environment.
 * Inorder to use that server configuration, we have to bind that property value to our
 * "spring.kafka.producer.bootstrap-servers".
 *
 * Similarly, we have to map "spring.kafka.admin.properties.bootstrap.servers" from
 * "spring.embedded.kafka.brokers"
 */
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
})
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
