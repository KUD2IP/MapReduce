package org.example;

import org.example.service.Coordinator;
import org.example.service.Worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class App 
{
    public static void main( String[] args ) {
        String[] files = {"src/main/resources/inputFiles/file1.txt", "src/main/resources/inputFiles/file2.txt", "src/main/resources/inputFiles/file3.txt"};

        Coordinator coordinator = new Coordinator(files);
        ExecutorService executor = Executors.newFixedThreadPool(files.length);

        for (int i = 0; i < files.length; i++) {
            executor.submit(new Worker(coordinator, coordinator.getNumReduceTasks()));
        }

        executor.shutdown();

        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
