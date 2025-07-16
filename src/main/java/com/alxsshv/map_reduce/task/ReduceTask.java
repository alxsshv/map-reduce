package com.alxsshv.map_reduce.task;

/**
 * Класс, описывающий reduce-задачу для worker'а
 * @author Alexei Shvariov
 */
public record ReduceTask (int id) implements Task {
}
