package com.techstack.kafka.unit.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.techstack.kafka.controller.LibraryEventsController;
import com.techstack.kafka.domain.Book;
import com.techstack.kafka.domain.LibraryEvent;
import com.techstack.kafka.producer.LibraryEventProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * When you use @WebMvcTest it would scan this Controller class.
 * It will not touch other Service / Bean classes
 */
@WebMvcTest(controllers = LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean //Controller depends on this LibraryEventProducer
    LibraryEventProducer libraryEventProducer;

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("Test Post Library Event")
    void postLibraryEvent() throws Exception {
        //Given
        Book book = Book.builder().bookId(123).bookAuthor("Karthi").bookName("TDD").build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String json = objectMapper.writeValueAsString(libraryEvent);

        //When
        when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        //Then
        mockMvc.perform(post("/v1/library-event")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
    }

    @Test
    @DisplayName("Test Post Library Event to verify 4xx")
    void postLibraryEvent_4xx() throws Exception {
        //Given
        Book book = Book.builder().bookId(null).bookAuthor(null).bookName("TDD").build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String json = objectMapper.writeValueAsString(libraryEvent);

        //When
        when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        //Then
        String expectedErrorMessage = "book.bookAuthor - must not be blank, book.bookId - must not be null";

        mockMvc.perform(post("/v1/library-event")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorMessage));
    }

    @Test
    void updateLibraryEvent() throws Exception {

        //given
        Book book = new Book().builder()
                .bookId(123)
                .bookAuthor("Karthi")
                .bookName("Kafka Using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(123)
                .book(book)
                .build();
        String json = objectMapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        //expect
        mockMvc.perform(
                put("/v1/library-event")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

    }

    @Test
    void updateLibraryEvent_withNullLibraryEventId() throws Exception {

        //given
        Book book = new Book().builder()
                .bookId(123)
                .bookAuthor("Karthi")
                .bookName("Kafka Using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        String json = objectMapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        //expect
        mockMvc.perform(
                put("/v1/library-event")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string("Please pass the LibraryEventId"));

    }
}
