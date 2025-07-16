package com.alxsshv.map_reduce;

import java.util.List;

/**
 * @author Alexei Shvariov
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        List<String> filenames = List.of("dost1.txt", "dost2.txt", "dost3.txt", "dost4.txt", "dost5.txt", "dost6.txt");
        Coordinator coordinator = new Coordinator(filenames, "results");
        coordinator.startWork();
    }
}
