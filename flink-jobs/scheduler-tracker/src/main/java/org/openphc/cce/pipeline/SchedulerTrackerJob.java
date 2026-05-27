package org.openphc.cce.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * Flink job: Reads scheduler triggers from cce.scheduler.triggers
 * and sinks step transitions to ClickHouse step_transitions table.
 *
 * Event format:
 * {
 *   "stepInstanceId": "UUID",
 *   "transitionType": "PENDING_TO_DUE | DUE_TO_OVERDUE | OVERDUE_TO_MISSED",
 *   "triggeredAt": 1778224037.910005746,  (epoch seconds with fractional)
 *   "correlationId": "sched-PENDING_TO_DUE-{stepInstanceId}-{timestamp}"
 * }
 */
public class SchedulerTrackerJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final OutputTag<String> DLQ_TAG = new OutputTag<>("dlq") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String clickhouseUrl = System.getenv().getOrDefault("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/cce_analytics");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("cce.scheduler.triggers")
                .setGroupId("cce-scheduler-tracker")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        var processed = kafkaStream.process(new SchedulerTransitionProcessor());

        processed.addSink(JdbcSink.sink(
                "INSERT INTO step_transitions (step_instance_id, transition_type, triggered_at, correlation_id) " +
                        "VALUES (?, ?, ?, ?)",
                (ps, record) -> {
                    ps.setString(1, record.stepInstanceId);
                    ps.setString(2, record.transitionType);
                    ps.setTimestamp(3, Timestamp.from(record.triggeredAt));
                    ps.setString(4, record.correlationId);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(500)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(clickhouseUrl)
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .build()
        )).name("clickhouse-step-transitions-sink");

        // DLQ
        processed.getSideOutput(DLQ_TAG)
                .sinkTo(org.apache.flink.connector.kafka.sink.KafkaSink.<String>builder()
                        .setBootstrapServers(kafkaBootstrap)
                        .setRecordSerializer(
                                org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
                                        .setTopic("cce.scheduler.triggers.dlq")
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build()
                        )
                        .build())
                .name("dlq-sink");

        env.execute("CCE Scheduler Tracker");
    }

    public static class SchedulerTransitionProcessor extends ProcessFunction<String, SchedulerRecord> {
        @Override
        public void processElement(String value, Context ctx, Collector<SchedulerRecord> out) {
            try {
                JsonNode root = MAPPER.readTree(value);
                SchedulerRecord record = new SchedulerRecord();
                record.stepInstanceId = root.get("stepInstanceId").asText();
                record.transitionType = root.get("transitionType").asText();
                record.correlationId = root.has("correlationId") ? root.get("correlationId").asText() : "";

                // triggeredAt is epoch seconds with fractional nanoseconds
                JsonNode triggeredAt = root.get("triggeredAt");
                if (triggeredAt.isNumber()) {
                    double epochSeconds = triggeredAt.asDouble();
                    long epochMillis = (long) (epochSeconds * 1000);
                    record.triggeredAt = Instant.ofEpochMilli(epochMillis);
                } else {
                    record.triggeredAt = Instant.parse(triggeredAt.asText());
                }

                out.collect(record);
            } catch (Exception e) {
                ctx.output(DLQ_TAG, value);
            }
        }
    }
}
