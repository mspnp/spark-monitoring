package com.microsoft.pnp.logging;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface JSONConfiguration {
    void configure(ObjectMapper objectMapper);
}
