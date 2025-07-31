package com.alxsshv.map_reduce.task;

/**
 * Класс, описывающий map-задачу для worker'а
 * @author Alexei Shvariov
 */
public record MapTask(String filename, int id) implements Task {

}
