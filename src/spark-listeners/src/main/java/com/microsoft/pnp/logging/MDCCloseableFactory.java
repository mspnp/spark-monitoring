package com.microsoft.pnp.logging;

import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MDCCloseableFactory {
    private class MDCCloseable implements AutoCloseable {
        @SuppressWarnings("unchecked")
        public MDCCloseable(Map<String, Object> mdc) {
            // Log4j supports Map<String, Object>, but slf4j wants Map<String, String>
            // Because of type erasure, this should be okay.
            MDC.setContextMap((Map)mdc);
        }

        @Override
        public void close() {
            MDC.clear();
        }
    }

    private Optional<Map<String, Object>> context;

    public MDCCloseableFactory() {
        this(null);
    }

    public MDCCloseableFactory(Map<String, Object> context) {
        this.context = Optional.ofNullable(context);
    }

    public AutoCloseable create(Map<String, Object> mdc) {
        // Values in mdc will override context
        Map<String, Object> newMDC = new HashMap<>();
        this.context.ifPresent(c -> newMDC.putAll(c));
        newMDC.putAll(mdc);
        return new MDCCloseable(newMDC);
    }
}
