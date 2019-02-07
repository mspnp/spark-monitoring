package com.microsoft.pnp.logging;

import org.apache.log4j.PropertyConfigurator;

import java.io.InputStream;

public class Log4jConfiguration {
    public static void configure(String configFilename) {
        PropertyConfigurator.configure(configFilename);
    }

    public static void configure(InputStream inputStream) {
        PropertyConfigurator.configure(inputStream);
    }
}
