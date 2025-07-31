package com.alxsshv.map_reduce;

import java.util.List;

/**
 * Класс демонстрирует работу упрощенной реализации фреймворка MapReduce.
 * @author Alexei Shvariov
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        /** Список текстовых файлов в которых подсчитываем слова */
        List<String> filenames = List.of("dost1.txt", "dost2.txt", "dost3.txt", "dost4.txt", "dost5.txt", "dost6.txt");
        Coordinator coordinator = new Coordinator(filenames, "results");
        coordinator.startWork();
    }
}
