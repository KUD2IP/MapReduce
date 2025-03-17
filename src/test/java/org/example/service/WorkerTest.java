package org.example.service;

import org.example.entity.KeyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WorkerTest {
    private Coordinator coordinator;
    private Worker worker;

    @BeforeEach
    void setUp() {
        coordinator = Mockito.mock(Coordinator.class);
        worker = new Worker(coordinator, 2);
    }

    @Test
    void testMapFunction() {
        List<KeyValue> result = worker.map("word1 word2 word1");
        assertEquals(3, result.size());
        assertEquals("word1", result.get(0).getKey());
        assertEquals("1", result.get(0).getValue());
    }

    @Test
    void testReduceFunction() {
        List<String> values = Arrays.asList("1", "1", "1");
        String result = worker.reduce(values);
        assertEquals("3", result);
    }
}
