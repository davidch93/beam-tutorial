package com.dch.tutorial.beam.transform;

import com.dch.tutorial.beam.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Transform the kafka record to specific record. The record used format from Debezium.
 *
 * @author david.christianto
 */
public class KafkaRecordTransformer extends PTransform<PCollection<String>, PCollection<String>> {

    private KafkaRecordTransformer() {
    }

    /**
     * A convenience way to construct {@link KafkaRecordTransformer}.
     *
     * @return {@link KafkaRecordTransformer}
     */
    public static KafkaRecordTransformer transform() {
        return new KafkaRecordTransformer();
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String record, OutputReceiver<String> out) {
                JsonNode payload = JsonUtil.toJsonNode(record).get("payload");
                out.output(payload.toString());
            }
        }));
    }
}
