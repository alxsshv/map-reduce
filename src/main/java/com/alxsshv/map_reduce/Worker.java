package com.alxsshv.map_reduce;

import com.alxsshv.map_reduce.result.KeyResult;
import com.alxsshv.map_reduce.result.KeyValue;
import com.alxsshv.map_reduce.task.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;


/**
 * Класс выполняющий алгоритм полученных задач в отдельном потоке.
 * @author Alexei Shvariov
 */
@Slf4j
public class Worker implements Callable<Boolean> {
    /**Числовой идентификатор */
    private final int id;
    /** Провайдер для взаимодействия с хранилищем файлов */
    private final StorageProvider storageProvider;
    /** Координатор, который создал процесс-worker */
    private final Coordinator coordinator;

    /** Конструктор с параметрами
     * @param coordinator - Координатор, который создал экземпляр worker'а.
     * @param id - идентификатор worker'а.
     * */
    public Worker(Coordinator coordinator, int id) {
        this.coordinator = coordinator;
        this.id = id;
        this.storageProvider = new StorageProvider();
    }

    /** Метод, выполняющийся при запуске worker'a в пуле потоков
     * Метод запрашивает задачи в координаторе и в зависимости от типа полученной задачи выполняет map-задачу,
     * reduce-задачу, ожидает новой задачи или завершает работу
     * @return возвращает true, если поток успешно завершил все операции*/
    @Override
    public Boolean call() {
        log.info("Запущен worker № {}", id);
        while (true) {
            log.info("Worker {}: запросил задачу", id);
            Task task = coordinator.getTask();
            if (task == null  || task instanceof WaitTask) {
                log.info("Worker {}: ожидаю", id);
                Thread.yield();
                continue;
            }
            if (task instanceof StopTask) {
                log.info("Worker {}: завершил работу", id);
                break;
            }
            if (task instanceof MapTask mapTask) {
                log.info("Worker {}: выполняю map задачу № {}", id, mapTask.id());
                final String content = storageProvider.getFileContent(mapTask.filename());
                final List<KeyValue> keyValues = map(content);
                storageProvider.writeMapResults(keyValues, mapTask, coordinator.getNumberOfReduceTasks());
                log.info("Worker {}: выполнена map задача № {}", id, mapTask.id());
                coordinator.mapTaskComplete();
                continue;
            }
            if (task instanceof ReduceTask reduceTask) {
                log.info("Worker {}: выполняю reduce задачу № {}", id, reduceTask.id());
                final List<KeyValue> keyValues = storageProvider.getDataForReduceTask(reduceTask.id());
                final Map<String, List<KeyValue>> mapValues = keyValues.stream().collect(Collectors.groupingBy(KeyValue::key));
                final StringBuilder reduceResult = new StringBuilder();
                for (String key: mapValues.keySet()) {
                    final KeyResult keyResult = reduce(key, mapValues.get(key));
                    reduceResult.append(keyResult).append(System.lineSeparator());
                }
                storageProvider.writeReduceResultToFile(reduceResult.toString(), coordinator.getResultFilename());
                log.info("Worker {}: выполнена reduce задача № {}", id, reduceTask.id());
            }
        }
        return true;
    }

    /** Метод считывает содержимое файла в виде строки, делит на слова и собирает в список {@link KeyValue} объектов
     *  для дальнейшего сохранения в промежуточный файл
     *  @param content - содержимое одного из файлов, в которых необходимо определить сколько раз встречается слово.
     *  @return возвращает список {@link KeyValue} */
    public List<KeyValue> map (String content) {
        return Arrays.stream(content.split(" "))
                .map(word -> word.replaceAll("[^a-zA-Zа-яА-Я0-9-]", ""))
                .map(word -> new KeyValue(word, 1))
                .toList();
    }


    /** Подсчитывает сколько раз стречается слово ключ в тексте и схораняет в {@link KeyResult} объект. */
    public KeyResult reduce(String key, List<KeyValue> values) {
        return new KeyResult(key, values.size());
    }

}
