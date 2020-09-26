package com.mika.kafka;

import java.util.concurrent.ThreadFactory;

public class NamingThreadFactory implements ThreadFactory{
    final String name;
    private int counter;

    NamingThreadFactory(String name) {
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, name + "-" + counter++);
    }

}
