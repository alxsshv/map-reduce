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
 * @author Alexei Shvariov
 */
@Slf4j
public class Worker implements Callable<Boolean> {
    /***/
    private final int id;
    /***/
    private final StorageProvider storageProvider;
    /***/
    private final Coordinator coordinator;

    /***/
    public Worker(Coordinator coordinator, int id) {
        this.coordinator = coordinator;
        this.id = id;
        this.storageProvider = new StorageProvider();
    }

    /***/
    @Override
    public Boolean call() {
        log.info("Запущен worker № {}", id);
        while (true) {
            log.info("Worker {}: запросил задачу", id);
            Task task = coordinator.getTask();
            if (task == null) {
                Thread.yield();
                continue;
            }
            if (task instanceof StopTask) {
                log.info("Worker {}: завершил работу", id);
                break;
            }
            if (task instanceof WaitTask) {
                log.info("Worker {}: ожидаю", id);
                Thread.yield();
                continue;
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

    /***/
    public List<KeyValue> map (String content) {
        return Arrays.stream(content.split(" "))
                .map(word -> word.replaceAll("[^a-zA-Zа-яА-Я0-9-]", ""))
                .map(word -> new KeyValue(word, 1))
                .toList();
    }


    /***/
    public KeyResult reduce(String key, List<KeyValue> values) {
        return new KeyResult(key, values.size());
    }


}
