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
 * @see org.apache.beam.sdk.transforms.SerializableFunction
 */
public class TombStoneRecordFilter implements SerializableFunction<String, Boolean> {

    private TombStoneRecordFilter() {
    }

    /**
     * A convenience way to construct {@link TombStoneRecordFilter}.
     *
     * @return {@link TombStoneRecordFilter}
     */
    public static TombStoneRecordFilter filter() {
        return new TombStoneRecordFilter();
    }

    @Override
    public Boolean apply(String input) {
        JsonNode value = JsonUtil.toJsonNode(input);
        return !value.get("schema").isNull() && !value.get("payload").isNull();
    }
}
