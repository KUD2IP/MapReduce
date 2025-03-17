package org.example.service;

import org.example.entity.KeyValue;
import org.example.entity.MapTask;
import org.example.entity.ReduceTask;
import org.example.entity.Task;

import java.io.*;
import java.util.*;

/**
 * Класс Worker реализует логику выполнения задач (map и reduce) в рамках MapReduce-программы.
 * Каждый Worker работает в отдельном потоке и запрашивает задачи у Coordinator.
 */
public class Worker implements Runnable {

    // Координатор, который управляет задачами.
    private final Coordinator coordinator;

    // Количество reduce-тасков.
    private final int numReduceTasks;


    public Worker(Coordinator coordinator, int numReduceTasks) {
        this.coordinator = coordinator;
        this.numReduceTasks = numReduceTasks;
    }

    @Override
    public void run() {
        while (true) {
            // Запрашиваем задачу у Coordinator.
            Task task = coordinator.getTask();

            // Если задач больше нет, завершаем выполнение.
            if (task == null) {
                break;
            }

            // Если задача типа MapTask, обрабатываем её.
            if (task instanceof MapTask) {
                processMapTask((MapTask) task);
            }
            // Если задача типа ReduceTask, обрабатываем её.
            else if (task instanceof ReduceTask) {
                processReduceTask((ReduceTask) task);
            }
        }
    }

    /**
     * Обрабатывает map-задачу.
     *
     * @param task Map-задача, которую необходимо выполнить.
     */
    private void processMapTask(MapTask task) {
        String fileName = task.getFileName();
        String content = readFile(fileName);

        // Применяем функцию map к содержимому файла.
        List<KeyValue> keyValues = map(content);

        // Группируем key-value пары по reduce-таскам.
        Map<Integer, List<String>> outputFiles = new HashMap<>();
        Map<Integer, String> intermediateFileNames = new HashMap<>();

        for (KeyValue kv : keyValues) {
            // Определяем reduce-таск для текущего ключа.
            int reduceTaskId = Math.abs(kv.getKey().hashCode()) % numReduceTasks;
            outputFiles.computeIfAbsent(reduceTaskId, k -> new ArrayList<>()).add(kv.getKey() + " " + kv.getValue());
        }

        // Записываем промежуточные файлы.
        for (Map.Entry<Integer, List<String>> entry : outputFiles.entrySet()) {
            int reduceTaskId = entry.getKey();
            String intermediateFileName = "mr-" + task.getId() + "-" + reduceTaskId + ".txt";
            intermediateFileNames.put(reduceTaskId, intermediateFileName);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(intermediateFileName))) {
                for (String line : entry.getValue()) {
                    writer.write(line + "\n");
                }
            } catch (IOException e) {
                throw new RuntimeException("Ошибка при чтении файла: " + fileName, e);
            }
        }
        // Сообщаем Coordinator о завершении map-задачи.
        coordinator.completeMapTask(task, intermediateFileNames);
    }

    /**
     * Обрабатывает reduce-задачу.
     *
     * @param task Reduce-задача, которую необходимо выполнить.
     */
    private void processReduceTask(ReduceTask task) {
        List<String> files = task.getFileNames();

        // Если файлов нет, завершаем выполнение.
        if (files.isEmpty()) {
            return;
        }

        // Читаем key-value пары из всех файлов.
        List<KeyValue> keyValues = new ArrayList<>();

        for (String file : files) {
            File intermediateFile = new File(file);

            // Если файл не существует, пропускаем его.
            if (!intermediateFile.exists()) {
                System.err.println("File not found: " + file);
                continue;
            }

            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Разделяем строку на ключ и значение.
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length >= 2) {
                        keyValues.add(new KeyValue(parts[0], parts[1]));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Ошибка при чтении файла: " + file, e);
            }
        }

        // Сортируем key-value по ключу.
        keyValues.sort(Comparator.comparing(KeyValue::getKey));

        // Записываем результаты в финальный файл.
        String outputFile = "output-" + task.getId() + ".txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            String currentKey = null;
            List<String> values = new ArrayList<>();

            for (KeyValue kv : keyValues) {
                // Если ключ изменился, применяем функцию reduce к предыдущему ключу.
                if (currentKey != null && !currentKey.equals(kv.getKey())) {
                    String result = reduce(values);
                    writer.write(currentKey + " " + result + "\n");
                    values.clear();
                }
                currentKey = kv.getKey();
                values.add(kv.getValue());
            }

            // Обрабатываем последний ключ.
            if (currentKey != null) {
                String result = reduce(values);
                writer.write(currentKey + " " + result + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Ошибка при чтении файла ", e);
        }
        // Сообщаем Coordinator о завершении reduce-задачи.
        coordinator.completeReduceTask(task);
    }

    /**
     * Читает содержимое файла.
     *
     * @param fileName Имя файла.
     * @return Содержимое файла в виде строки.
     */
    private String readFile(String fileName) {
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Ошибка при чтении файла: " + fileName, e);
        }
        return content.toString();
    }

    /**
     * Применяет функцию map к содержимому файла.
     *
     * @param content Содержимое файла.
     * @return Список key-value пар.
     */
    public List<KeyValue> map(String content) {
        List<KeyValue> keyValues = new ArrayList<>();
        String[] words = content.split("\\s+");
        for (String word : words) {
            if (!word.isEmpty()) {
                keyValues.add(new KeyValue(word, "1"));
            }
        }
        return keyValues;
    }

    /**
     * Применяет функцию reduce к списку значений.
     *
     * @param values Список значений.
     * @return Результат reduce-функции.
     */
    public String reduce(List<String> values) {
        return String.valueOf(values.size());
    }
}