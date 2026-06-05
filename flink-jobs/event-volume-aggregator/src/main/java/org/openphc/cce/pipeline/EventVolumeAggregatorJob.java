package org.openphc.cce.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

/**
 * Flink job: Aggregates event counts in 1-hour tumbling windows
 * keyed by (facility_id, source, resource_type) → event_volume_hourly.
 */
public class EventVolumeAggregatorJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String clickhouseUrl = System.getenv().getOrDefault("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/cce_analytics");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("cce.events.inbound")
                .setGroupId("cce-event-volume-aggregator")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                .withTimestampAssigner((SerializableTimestampAssigner<String>) (event, ts) -> {
                    try {
                        JsonNode root = MAPPER.readTree(event);
                        String time = root.has("time") ? root.get("time").asText() : null;
                        return time != null ? Instant.parse(time).toEpochMilli() : ts;
                    } catch (Exception e) {
                        return ts;
                    }
                });

        DataStream<String> kafkaStream = env.fromSource(source, watermarkStrategy, "kafka-source");

        kafkaStream
                .map(EventVolumeAggregatorJob::extractKey)
                .filter(t -> t != null)
                .keyBy(t -> t.f0 + "|" + t.f1 + "|" + t.f2 + "|" + t.f3)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .allowedLateness(Time.minutes(5))
                .apply(new CountWindowFunction())
                .addSink(JdbcSink.sink(
                        "INSERT INTO event_volume_hourly (hour, facility_id, source, event_type, resource_type, event_count) " +
                                "VALUES (?, ?, ?, ?, ?, ?)",
                        (ps, record) -> {
                            ps.setTimestamp(1, new Timestamp(record.f4));
                            ps.setString(2, record.f0);
                            ps.setString(3, record.f1);
                            ps.setString(4, record.f2);
                            ps.setString(5, record.f3);
                            ps.setLong(6, record.f5);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(500)
                                .withBatchIntervalMs(10000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(clickhouseUrl)
                                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                .build()
                )).name("clickhouse-volume-sink");

        env.execute("CCE Event Volume Aggregator");
    }

    /**
     * Extracts (facility_id, source, event_type, resource_type, eventTimeMs) from CloudEvent JSON.
     */
    static Tuple5<String, String, String, String, Long> extractKey(String json) {
        try {
            JsonNode root = MAPPER.readTree(json);
            String facilityId = root.has("facilityid") ? root.get("facilityid").asText() : "unknown";
            String source = root.has("source") ? root.get("source").asText() : "unknown";
            String eventType = root.has("type") ? root.get("type").asText() : "unknown";
            String resourceType = "unknown";
            JsonNode data = root.get("data");
            if (data != null && data.has("resourceType")) {
                resourceType = data.get("resourceType").asText();
            }
            long eventTime = root.has("time") ? Instant.parse(root.get("time").asText()).toEpochMilli() : System.currentTimeMillis();
            return Tuple5.of(facilityId, source, eventType, resourceType, eventTime);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Window function that counts events and emits (facility_id, source, event_type, resource_type, window_start_ms, count).
     */
    private static class CountWindowFunction implements WindowFunction<
            Tuple5<String, String, String, String, Long>,
            Tuple6<String, String, String, String, Long, Long>,
            String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window,
                          Iterable<Tuple5<String, String, String, String, Long>> input,
                          Collector<Tuple6<String, String, String, String, Long, Long>> out) {
            long count = 0;
            String facilityId = null, source = null, eventType = null, resourceType = null;
            for (var t : input) {
                facilityId = t.f0;
                source = t.f1;
                eventType = t.f2;
                resourceType = t.f3;
                count++;
            }
            out.collect(Tuple6.of(facilityId, source, eventType, resourceType, window.getStart(), count));
        }
    }
}
