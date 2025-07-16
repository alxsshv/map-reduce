package com.alxsshv.map_reduce.result;

/**
 *  Пара ключ-значение, где key - слово, количество повторений которого необходимо посчитать,
 *  value - сколько раз слово встречается в исходных файлах
 * @author Alexei Shvariov
 */
public record KeyResult(String key, int value) {
    @Override
    public String toString() {
        return key.concat(" - ").concat(String.valueOf(value));
    }
}
