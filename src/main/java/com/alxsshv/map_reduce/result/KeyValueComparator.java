package com.alxsshv.map_reduce.result;

import java.util.Comparator;

/**
 * Класс сравнения двух объектов класса {@link KeyValue}
 * @author Alexei Shvariov
 */
public class KeyValueComparator implements Comparator<KeyValue> {
    /** Метод сравнения двух объектов класса {@link KeyValue} */
    @Override
    public int compare(KeyValue o1, KeyValue o2) {
        return o1.key().compareTo(o2.key());
    }
}
