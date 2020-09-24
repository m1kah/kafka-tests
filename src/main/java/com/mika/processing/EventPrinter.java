package com.mika.processing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventPrinter {
    private static final Logger log = LoggerFactory.getLogger(EventPrinter.class);
    public void print(String event) {
        log.info(event);
    }
}
