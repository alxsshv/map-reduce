package com.alxsshv.map_reduce;

import com.alxsshv.map_reduce.task.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Координатор - класс раздающий задачи для объектов класса {@link Worker} в рамках выполнения MapReduce процесса
 * подсчёта количества слов встречающихся в файлах, указанных при создании координатора.
 * @author Alexei Shvariov
 */
@Getter
@Slf4j
public class Coordinator {
    /** Очередь map-задач {@link MapTask} */
    private final Deque<MapTask> mapTasks = new ArrayDeque<>();
    /** Очередь на выполнение reduce-задач {@link ReduceTask} */
    private final Deque<ReduceTask> reduceTasks = new ArrayDeque<>();
    /** Количество выполненных map-задач */
    private final AtomicInteger mapTasksIsDone = new AtomicInteger(0);
    /** Пул потоков для управления worker'ами */
    private final ExecutorService pool;
    /** Количество создаваемых воркеров для выполнения map-reduce процесса.
     * Параметр определяется пользователем или устанавливается равным количеству файлов,
     * в которых необходимо считать слова*/
    private final int numberOfWorkers;
    /** Количество map-задач устанавливается равным количеству файлов для обработки */
    private final int numberOfMapTasks;
    /** Количество reduce-задач, определяется пользователем или устанавливается по умолчанию равным
     *  количеству файлов для обработки*/
    private final int numberOfReduceTasks;
    /** Имя файла с результатами выполнения MapReduce процесса. */
    private final String resultFilename;

    /**Конструктор с параметрами
     * @param filenames - список имён файлов для обработки, указываается с учётом пути к файлам.
     * @param resultFilename  - имя файла в который будут записаны результаты выполнения MapReduce процесса.
     * @param numberOfWorkers  - требуемое количество worker'ов.
     * @param numberOfReduceTasks - треуемое количество reduce-задач.
     * @param parallelism - уровень распараллеливания задач пулом потоков;
     * */
    public Coordinator(List<String> filenames,
                        String resultFilename,
                       int numberOfWorkers,
                       int numberOfReduceTasks,
                       int parallelism) {
        if (!filenames.isEmpty()) {
            int i = 0;
            for (String filename: filenames) {
                if (filename != null && !filename.isEmpty()) {
                    this.mapTasks.offer(new MapTask(filename, ++i));
                }
            }
        }
        this.resultFilename = resultFilename;
        this.numberOfWorkers =  numberOfWorkers > 1 ?  numberOfWorkers : filenames.size();
        this.numberOfReduceTasks = numberOfReduceTasks > 0 ? numberOfReduceTasks : filenames.size();
        this.numberOfMapTasks = filenames.size();
        this.pool = parallelism > 1 ? Executors.newWorkStealingPool(parallelism) : Executors.newWorkStealingPool();
    }

    /**Конструктор с параметрами
     * @param filenames - список имён файлов для обработки, указываается с учётом пути к файлам.
     * @param resultFilename  - имя файла в который будут записаны результаты выполнения MapReduce процесса.
     * @param numberOfWorkers  - требуемое количество worker'ов.
     * @param numberOfReduceTasks - треуемое количество reduce-задач.
     * */
    public Coordinator(List<String> filenames,
                       String resultFilename,
                       int numberOfWorkers,
                       int numberOfReduceTasks) {
        this(filenames, resultFilename, numberOfWorkers, numberOfReduceTasks, Runtime.getRuntime().availableProcessors());
    }

    /**Конструктор с параметрами
     * @param filenames - список имён файлов для обработки, указываается с учётом пути к файлам.
     * @param resultFilename  - имя файла в который будут записаны результаты выполнения MapReduce процесса.
     * */
    public Coordinator(List<String> filenames,
                       String resultFilename) {
        this(filenames, resultFilename,
               filenames.size(),
                Runtime.getRuntime().availableProcessors() * 3,
                Runtime.getRuntime().availableProcessors());
    }


    /**Метод для запуска MapReduce процесса на выполнение.
     * Метод очищает каталоги с генерируемыми файлами (промежуточныими и результатом выполнения)
     * от старых данных, генерирует очереть reduce- задач и запускает worker'ы (метод call worker'а выполняется в отдельном потоке).
     * */
    public void startWork() throws InterruptedException {
        new StorageProvider().prepareStorage();
        for (int i = 1; i <= numberOfReduceTasks; i++) {
            this.reduceTasks.offer(new ReduceTask(i));
        }
        final List<Worker> workers = new ArrayList<>();
        for (int i = 1; i <= numberOfWorkers; i++) {
            Worker worker = new Worker(this, i);
            workers.add(worker);
        }
        log.info("Coordinator: Создано {} workers", workers.size());
        pool.invokeAll(workers);
    }

    /**Метод выдачи задачи на выполнение Worker'ам.
     * @return задача, реализующая интерфейс {@link Task}. Интрефейс, а не абстрактный класс т.к.
     * в качестве реализаций используются иммутабельные record'ы.
     * Если имеются не выполненные map-задачи кординатор возвращает по запросу одну map-задачу {@link MapTask} из очереди map-задач
     * Если свободных map-задач нет, но некоторые из них еще выполняются,
     * возвращается задача на ожидание {@link WaitTask}.
     * Если все map-задачи выполнены, а в очереди reduce-задач имются задачи? возвращается {@link ReduceTask}
     * Если все map-задачи выполнены, а reduce-задач в очереди больше нет,
     * взворащает задача на завершения работы Worker'a {@link StopTask}*/
    public synchronized Task getTask() {
        if (!mapTasks.isEmpty()) {
            return mapTasks.poll();
        }
        if (reduceTasks.size() == numberOfReduceTasks && mapTasksIsDone.get() != numberOfMapTasks) {
            log.info("Coordinator: Выданы все map задачи");
            return new WaitTask();
        }
        if (mapTasksIsDone.get() == numberOfMapTasks && !reduceTasks.isEmpty()) {
            log.info("Coordinator: Выполнены все map задачи");
            log.info("Coordinator: В очереди {} reduce задач", reduceTasks.size() );
            return reduceTasks.poll();
        }
        log.info("Coordinator: Выданы все reduce задачи");
        return new StopTask();
    }

    /** Метод, увеличивающий на один счётчик выполненных map задач */
    public void mapTaskComplete() {
        mapTasksIsDone.incrementAndGet();
    }

}
