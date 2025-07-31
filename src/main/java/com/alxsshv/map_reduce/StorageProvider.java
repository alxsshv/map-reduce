package com.alxsshv.map_reduce;

import com.alxsshv.map_reduce.result.KeyValue;
import com.alxsshv.map_reduce.result.KeyValueComparator;
import com.alxsshv.map_reduce.result.KeyValueParser;
import com.alxsshv.map_reduce.task.MapTask;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Класс, содержащий меотод работы с файлами при выполнениия MapReduce процесса.
 * @author Alexei Shvariov
 */
public class StorageProvider {
    /** Каталог для хранения промежуточных результатов выполнения map-задач */
    private static final String MAP_RESULTS_DIRECTORY = "map_results";
    /** Каталог для хранения результатов MapReduce процесса. */
    private static final String RESULT_FILE_PATH = "result";
    /** Префикс наименования файла с промежуточными результатами выполнения
     *  map операции для идентификации map-задачи в ходе выполнения которой создан файл*/
    private static final String MAP_PREFIX = "map_";
    /** Префикс наименования файла с промежуточными результатами выполнения
     *  map операции для идентификации reduce-задачи,
     *  в которой должна осуществляться обработка промежуточных результатов*/
    private static final String REDUCE_PREFIX = "reduce_";
    /**Расширение для файлов*/
    private static final String FILE_EXTENSION = ".txt";

    /**Метод подготовки хранилища файлов.
     * Создает необходимые каталоги (если их нет) и очищает их если они не пусты*/
    public void prepareStorage() {
        createDirectoryIfNotExist(MAP_RESULTS_DIRECTORY);
        clearDirectory(new File(MAP_RESULTS_DIRECTORY));
        createDirectoryIfNotExist(RESULT_FILE_PATH);
        clearDirectory(new File(RESULT_FILE_PATH));
    }

    /** Метод создания каталога если его не существует.
     * @param directoryPath - путь к каталогу, который необходимо создать */
    private void createDirectoryIfNotExist(String directoryPath) {
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    /** Метод, который удаляет все файлы в казанном каталогу.
     * @param directory - каталог из которого необходимо удалить файлы */
    private void clearDirectory(File directory) {
        if (directory.isDirectory() || directory.listFiles().length != 0) {
            File[] directoryFiles = directory.listFiles();
            for (File file : directoryFiles) {
                if (file != null) {
                    file.delete();
                }
            }
        }
    }

    /** Метод возвращает содержимое файла в виде строки.
     * @param filename  - имя файла, содержимое которого необходимо получить.
     * @return данные, хранящиеся в файле в виде строки.
     * @throws RuntimeException будет выброшено,
     * если возникнет ошибка при чтении файла или доступа к нему */
    public String getFileContent(String filename) {
        try {
            return String.valueOf(Files.readAllLines(Path.of(filename)));
        } catch (IOException ex) {
            throw new RuntimeException("Ошибка чтения файла:" + ex.getMessage());
        }
    }

    /** Записывает в файл промежуточные результаты, полученные в ходе выполнения указанной map-задачи.
     * @param keyValues  - список объектов класса {@link KeyValue}, полученных при выполнении map-задачи.
     * @param mapTask - map-задача {@link MapTask} при выполнении которой получены результаты, записываемые в файл.
     * @param numberOfReduceTasks - количество reduce-задач которые должны быть выполнены в ходе MapReduce процесса. */
    public void writeMapResults(List<KeyValue> keyValues, MapTask mapTask, int numberOfReduceTasks) {
        for (KeyValue kv : keyValues) {
            createDirectoryIfNotExist(MAP_RESULTS_DIRECTORY);
            String filePath = MAP_RESULTS_DIRECTORY + File.separator
                    + MAP_PREFIX + mapTask.id() + "-" + REDUCE_PREFIX + buildReduceTaskNumber(kv.key(), numberOfReduceTasks)
                    + FILE_EXTENSION;
          writeFile(filePath, kv + System.lineSeparator());
        }
    }

    /** Вычисляет номер reduce-задачи, которая должна обрабатывать данные по указанному ключу.
     * @param key  - ключ (слово) количество повторений которого в файлах будет вычисляться в reduce-задаче.
     * @param numberOfReduceTasks - количество reduce-задач которые должны быть выполнены в ходе MapReduce процесса.
     * @return возвращает строку, которая содержит номер reduce-задачи. */
    private String buildReduceTaskNumber(String key, int numberOfReduceTasks) {
        return String.valueOf( ((numberOfReduceTasks - 1) & key.hashCode()) + 1);
    }

    /**
     * Метод получения из файлов с промежуточными результатами выполнения map-задач списка пар ключ-значение,
     * которые должны обрабатываться в reduce-задаче с указанным номером.
     * @param reduceTaskNumber  - номер задачи для которой необходимо получить список пар ключ-значение.
     * @return возвращает коллекцию объектов {@link KeyValue}.
     * */
    public List<KeyValue> getDataForReduceTask(int reduceTaskNumber) {
        List<String> fileNames = getFilenamesForReduceTask(reduceTaskNumber);
        List<KeyValue> keyValues = new ArrayList<>();
        for (String fileName : fileNames) {
            keyValues.addAll(readMapResultFile(fileName));
        }
        keyValues.sort(new KeyValueComparator());
        return keyValues;
    }

    /**
     * Метод получает список имен файлов из которых необходимо получить промежуточные данные
     * для выполнения reduce-задачи с указанным номером.
     * @param reduceTaskNumber  - номер задачи для которой необходимо получить список фалов.
     * @return возвращает коллекцию имен файлов в которых хранятся промежуточные результаты выполнения map-задач,
     * которые должны быть обработаны при выполнении reduce-задачи с указанным в параметре номером.
     * */
    private List<String> getFilenamesForReduceTask(int reduceTaskNumber) {
        File mapFileDirectory = new File(MAP_RESULTS_DIRECTORY);
        if (!mapFileDirectory.exists()) {
            throw new RuntimeException("Каталог с промежуточными результатами не существует");
        }
        return Arrays.stream(Objects.requireNonNull(mapFileDirectory.listFiles()))
                .map(File::getName)
                .filter(name -> name.contains(REDUCE_PREFIX + reduceTaskNumber + FILE_EXTENSION)).toList();
    }

    /*** Метод получения из файла с промежуточными результатами выполнения map-задач списка пар ключ-значение.
     * @param fileName - имя файла из которого необходимо извлечь данные.
     * @return возвращает список объектов {@link KeyValue}
     * @throws RuntimeException будет выброшено,
     * если возникнет ошибка при чтении файла или доступа к нему.
     * */
    private List<KeyValue> readMapResultFile(String fileName) {
        final List<KeyValue> keyValues = new ArrayList<>();
        try {
            Scanner scanner = new Scanner(new FileReader(MAP_RESULTS_DIRECTORY + File.separator + fileName));
            while (scanner.hasNextLine()) {
                KeyValue keyValue = KeyValueParser.parseOf(scanner.nextLine());
                if (keyValue != null) {
                    keyValues.add(keyValue);
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Ошибка чтения файла: " + ex.getMessage());
        }
        return keyValues;
    }

    /**
     * Метод записи результата выполнения reduce-задачи в указанный файл.
     * @param reduceResult  - результаты выполнения reduce-задачи в виде строки.
     * @param filename - имя файла в который необходимо записать результаты выполнения reduce-задачи.
     * */
    public synchronized void writeReduceResultToFile(String reduceResult, String filename) {
        String filePath = RESULT_FILE_PATH + File.separator + filename + FILE_EXTENSION;
        writeFile(filePath, reduceResult);
    }

    /** Метод записи данных в файл
     * @param filePath - путь к фалу в который необходимо записать данные.
     * @param data - данные, которые необходимо записать в файл, в виде строки
     * @throws RuntimeException будет выброшено,
     * если возникнет ошибка при записи в файл или доступе к нему.
     * */
    private void writeFile(String filePath, String data) {
        try (FileWriter writer = new FileWriter(filePath, true)) {
            writer.write(data);
            writer.flush();
        } catch (IOException ex) {
            throw new RuntimeException("Ошибка записи файл: " + ex.getMessage());
        }
    }
}
