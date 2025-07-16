package com.alxsshv.map_reduce.result;

/**
 * @author Alexei Shvariov
 */
public record KeyResult(String key, int value) {
    @Override
    public String toString() {
        return key.concat(" - ").concat(String.valueOf(value));
    }
}
