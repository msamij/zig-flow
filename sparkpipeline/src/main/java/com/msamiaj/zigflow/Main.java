package com.msamiaj.zigflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        System.out.println("Hello world!");

        logger.debug("This is a debug message");
        logger.info("This is an info message");
        logger.warn("This is a warning message");
        logger.error("This is an error message");

        try {
            throw new Exception("An example exception");
        } catch (Exception e) {
            logger.error("Caught an exception", e);
        }
    }
}
