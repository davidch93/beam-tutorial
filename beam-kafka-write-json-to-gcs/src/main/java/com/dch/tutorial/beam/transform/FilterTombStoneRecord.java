package com.dch.tutorial.beam.transform;

import com.dch.tutorial.beam.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Filters the raw data which is tombstone flag or not. The record used format from Debezium.
 * Example of tombstone data:
 * <pre>
 *     {
 *         "schema": null,
 *         "payload": null
 *     }
 * </pre>
 *
 * @author david.christianto
 */
public class FilterTombStoneRecord implements SerializableFunction<String, Boolean> {

    @Override
    public Boolean apply(String input) {
        JsonNode value = JsonUtil.toJsonNode(input);
        return !value.get("schema").isNull() && !value.get("payload").isNull();
    }
}
