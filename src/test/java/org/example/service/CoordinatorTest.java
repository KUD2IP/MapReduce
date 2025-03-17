package org.example.service;

import org.example.entity.MapTask;
import org.example.entity.ReduceTask;
import org.example.entity.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.*;

class CoordinatorTest {
    private Coordinator coordinator;

    @BeforeEach
    void setUp() {
        String[] files = {"src/test/resources/test1.txt", "src/test/resources/test2.txt"};
        coordinator = new Coordinator(files);
    }

    @Test
    void testGetTaskReturnsMapTask() {
        Task task = coordinator.getTask();
        assertNotNull(task);
        assertTrue(task instanceof MapTask);
    }

    @Test
    void testCompleteMapTaskCreatesReduceTasks() {
        MapTask mapTask = new MapTask(1, "src/test/resources/test.txt", 2);
        Map<Integer, String> intermediateFiles = new HashMap<>();
        intermediateFiles.put(0, "mr-1-0.txt");
        intermediateFiles.put(1, "mr-1-1.txt");

        coordinator.completeMapTask(mapTask, intermediateFiles);
        assertFalse(coordinator.getTask() instanceof ReduceTask);
    }
}