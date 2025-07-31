package com.alxsshv.map_reduce.result;

/**
 * Парсер преобразуюущий строку в {@link KeyValue}
 * @author Alexei Shvariov
 */
public class KeyValueParser {

    /**Метод преобразвания строки в {@link KeyValue}
     * @param string - строк из которой необходимо получить пару ключ-значение.
     * @return {@link KeyValue} или null */
    public static KeyValue parseOf(String string) {
        if (string.contains(":")) {
            String[] keyAndValue = string.split(":");
            return new KeyValue(keyAndValue[0], Integer.parseInt(keyAndValue[1]));
        }
        return null;
    }
}
