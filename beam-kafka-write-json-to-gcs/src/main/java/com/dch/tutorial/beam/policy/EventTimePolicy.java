package com.dch.tutorial.beam.policy;

import com.dch.tutorial.beam.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Optional;

/**
 * A {@link TimestampPolicy} that assigns event time to each record.
 * The watermark for each Kafka partition is the timestamp of the last record read.
 * If a partition is idle, the watermark advances roughly to 'current time - 2 seconds'
 *
 * @author david.christianto
 */
public class EventTimePolicy extends TimestampPolicy<String, String> {

    private static final Duration IDLE_WATERMARK_DELTA = Duration.standardSeconds(2L);
    private Instant currentWatermark;

    private EventTimePolicy(Optional<Instant> previousWatermark) {
        this.currentWatermark = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    /**
     * A convenience way to construct {@link EventTimePolicy}.
     *
     * @param previousWatermark {@link Instant} Previous watermark.
     * @return {@link EventTimePolicy}
     */
    public static EventTimePolicy withEventTime(Optional<Instant> previousWatermark) {
        return new EventTimePolicy(previousWatermark);
    }

    @Override
    public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<String, String> record) {
        JsonNode payload = JsonUtil.toJsonNode(record.getKV().getValue()).get("payload");
        JsonNode body = !payload.get("op").asText().equalsIgnoreCase("d") ? payload.get("after") :
                payload.get("before");
        long eventTime = body.has("updated_at") ? body.get("updated_at").asLong() : payload.get("ts_ms").asLong();
        currentWatermark = new Instant(eventTime);
        return currentWatermark;
    }

    @Override
    public Instant getWatermark(PartitionContext context) {
        if (context.getMessageBacklog() == 0L) {
            Instant idleWatermark = context.getBacklogCheckTime().minus(IDLE_WATERMARK_DELTA);
            if (idleWatermark.isAfter(currentWatermark)) {
                currentWatermark = idleWatermark;
            }
        }

        return currentWatermark;
    }
}
