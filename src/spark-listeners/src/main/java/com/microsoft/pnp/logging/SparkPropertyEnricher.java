package com.microsoft.pnp.logging;

import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import com.microsoft.pnp.SparkInformation;

import java.util.Map;

import scala.collection.JavaConverters;

public class SparkPropertyEnricher extends Filter {

    @Override
    public int decide(LoggingEvent loggingEvent) {
        // This is not how we should really do this since we aren't actually filtering,
        // but because Spark uses the log4j.properties configuration instead of the XML
        // configuration, our options are limited.

        // There are some things that are unavailable until a certain point
        // in the Spark lifecycle on the driver.  We will try to get as much as we can.
        Map<String, String> javaMap = JavaConverters
                .mapAsJavaMapConverter(SparkInformation.get()).asJava();
        for (Map.Entry<String, String> entry : javaMap.entrySet()) {
            loggingEvent.setProperty(entry.getKey(), entry.getValue());
        }

        return Filter.NEUTRAL;
    }
}
