package org.example.service;

import lombok.Getter;
import org.example.entity.MapTask;
import org.example.entity.ReduceTask;
import org.example.entity.Task;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Класс Coordinator управляет задачами в рамках MapReduce-программы.
 * Он распределяет задачи между воркерами, отслеживает их выполнение
 * и управляет промежуточными файлами.
 */
public class Coordinator {

    // Очередь задач типа MapTask, которые необходимо выполнить.
    private final Queue<MapTask> mapTasks;

    // Очередь задач типа ReduceTask, которые необходимо выполнить.
    private final Queue<ReduceTask> reduceTasks;

    // Количество reduce-тасков, которое определяется автоматически на основе количества слов в файлах.
    @Getter
    private final int numReduceTasks;

    // Мапа, которая хранит имена промежуточных файлов, сгруппированных по номеру reduce-таска.
    private final Map<Integer, List<String>> intermediateFiles;

    // Множество идентификаторов завершенных map-тасков.
    private final Set<Integer> completedMapTasks;

    // Множество идентификаторов завершенных reduce-тасков.
    private final Set<Integer> completedReduceTasks;

    // Счетчик для генерации уникальных идентификаторов map-тасков.
    private final AtomicInteger nextMapTaskId = new AtomicInteger(0);

    // Общее количество map-задач.
    private final int totalMapTasks;

    /**
     * @param files Массив имен файлов, которые необходимо обработать.
     */
    public Coordinator(String[] files) {
        this.mapTasks = new ConcurrentLinkedQueue<>();
        this.reduceTasks = new ConcurrentLinkedQueue<>();
        this.intermediateFiles = new ConcurrentHashMap<>();
        this.completedMapTasks = ConcurrentHashMap.newKeySet();
        this.completedReduceTasks = ConcurrentHashMap.newKeySet();

        // Подсчет общего количества слов во всех файлах.
        int totalWords = countTotalWords(files);

        // Определение количества reduce-тасков (1 reduce-таск на каждые 2 слова).
        int wordsPerReduceTask = 2;
        this.numReduceTasks = Math.max(1, totalWords / wordsPerReduceTask);

        // Создание map-задач для каждого файла.
        for (String file : files) {
            mapTasks.add(new MapTask(nextMapTaskId.getAndIncrement(), file, numReduceTasks));
        }

        this.totalMapTasks = mapTasks.size();
    }

    /**
     * Подсчитывает общее количество слов во всех файлах.
     *
     * @param files Массив имен файлов.
     * @return Общее количество слов.
     */
    private int countTotalWords(String[] files) {
        int totalWords = 0;
        for (String file : files) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                // Чтение файла построчно.
                while ((line = reader.readLine()) != null) {
                    // Разделение строки на слова и подсчет их количества.
                    totalWords += line.split("\\s+").length;
                }
            } catch (IOException e) {
                throw new RuntimeException("Ошибка при чтении файла: " + file, e);
            }
        }
        return totalWords;
    }

    /**
     * Возвращает задачу для выполнения воркером.
     *
     * @return Задача типа MapTask или ReduceTask. Если задачи отсутствуют, возвращает null.
     */
    public synchronized Task getTask() {
        // Если есть map-задачи, возвращаем первую из очереди.
        if (!mapTasks.isEmpty()) {
            return mapTasks.poll();
        }
        // Если map-задачи завершены, но есть reduce-задачи, возвращаем первую reduce-задачу.
        else if (mapTasks.isEmpty() && !reduceTasks.isEmpty()) {
            return reduceTasks.poll();
        }
        // Если все задачи завершены, возвращаем null.
        else {
            return null;
        }
    }

    /**
     * Отмечает map-задачу как завершенную и добавляет промежуточные файлы в коллекцию.
     *
     * @param task Завершенная map-задача.
     * @param intermediateFileNames Имена промежуточных файлов, созданных в рамках этой задачи.
     */
    public synchronized void completeMapTask(MapTask task, Map<Integer, String> intermediateFileNames) {
        // Добавляем имена промежуточных файлов в мапу.
        for (Map.Entry<Integer, String> entry : intermediateFileNames.entrySet()) {
            int reduceTaskId = entry.getKey();
            String fileName = entry.getValue();
            intermediateFiles.computeIfAbsent(reduceTaskId, k -> new CopyOnWriteArrayList<>()).add(fileName);
        }

        // Отмечаем задачу как завершенную.
        completedMapTasks.add(task.getId());

        // Если все map-задачи завершены, создаем reduce-задачи.
        if (completedMapTasks.size() == totalMapTasks) {
            for (int i = 0; i < numReduceTasks; i++) {
                reduceTasks.add(new ReduceTask(i, intermediateFiles.getOrDefault(i, Collections.emptyList())));
            }
        }
    }

    /**
     * Отмечает reduce-задачу как завершенную.
     *
     * @param task Завершенная reduce-задача.
     */
    public synchronized void completeReduceTask(ReduceTask task) {
        // Отмечаем задачу как завершенную.
        completedReduceTasks.add(task.getId());
    }
}