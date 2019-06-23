package com.dch.tutorial.beam.transform;

import com.dch.tutorial.beam.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;

/**
 * Transform the kafka record to specific record. The record used format from Debezium.
 *
 * @author david.christianto
 * @see org.apache.beam.sdk.transforms.PTransform
 */
public class KafkaRecordTransformer extends PTransform<PCollection<String>, PCollection<GenericRecord>> {

    public static final Schema SCHEMA = SchemaBuilder.record("dbz_payload")
            .fields()
            .name("ts_ms").type(Schema.create(Schema.Type.LONG)).noDefault()
            .name("op").type(Schema.create(Schema.Type.STRING)).noDefault()
            .name("source").type(Schema.create(Schema.Type.STRING)).noDefault()
            .name("before").type(Schema.create(Schema.Type.STRING)).noDefault()
            .name("after").type(Schema.create(Schema.Type.STRING)).noDefault()
            .endRecord();
    private static final DatumReader<GenericRecord> avroReader = new GenericDatumReader<>(SCHEMA);

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
    public PCollection<GenericRecord> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new DoFn<String, GenericRecord>() {
            @ProcessElement
            public void processElement(@Element String record, OutputReceiver<GenericRecord> out) throws IOException {
                String payload = constructJson(JsonUtil.toJsonNode(record).get("payload"));
                Decoder decoder = DecoderFactory.get().jsonDecoder(SCHEMA, payload);
                out.output(avroReader.read(null, decoder));
            }
        }));
    }

    /**
     * Convert Debezium payload format for column `source`, `before`, `after` into string.
     *
     * @param payload {@link JsonNode} Debezium payload json.
     * @return Formatted json.
     */
    private String constructJson(JsonNode payload) {
        return JsonUtil.createEmptyObjectNode()
                .put("ts_ms", payload.get("ts_ms").asLong())
                .put("op", payload.get("op").asText())
                .put("source", payload.get("source").toString())
                .put("before", payload.get("before").toString())
                .put("after", payload.get("after").toString())
                .toString();
    }
}
