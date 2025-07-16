package com.alxsshv.map_reduce.result;

/**
 * Пара ключ-значение, где ключ - слово, количество повторений которого необходимо посчитать
 * Заначение всегда равно единице
 * @author Alexei Shvariov
 */
public record KeyValue(String key, int value) {
    /** Метод преобразования объекта в строку */
    @Override
    public String toString() {
        return key.concat(":").concat(String.valueOf(value));
    }
}
