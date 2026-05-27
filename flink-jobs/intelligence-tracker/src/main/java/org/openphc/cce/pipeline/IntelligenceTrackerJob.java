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
import java.util.UUID;

/**
 * Flink job: Reads intelligence triggers from cce.intelligence.triggers
 * and sinks to ClickHouse intelligence_events table.
 */
public class IntelligenceTrackerJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final OutputTag<String> DLQ_TAG = new OutputTag<>("dlq") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String clickhouseUrl = System.getenv().getOrDefault("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/cce_analytics");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("cce.intelligence.triggers")
                .setGroupId("cce-intelligence-tracker")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        var processed = kafkaStream.process(new IntelligenceEventProcessor());

        processed.addSink(JdbcSink.sink(
                "INSERT INTO intelligence_events (id, subject, intelligence_event_id, action_definition_id, " +
                        "protocol_definition_id, action_type, severity, intelligence_destination, step_state, " +
                        "action_id, protocol_canonical, detected_at) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, record) -> {
                    ps.setString(1, record.id);
                    ps.setString(2, record.subject);
                    ps.setString(3, record.intelligenceEventId);
                    ps.setString(4, record.actionDefinitionId);
                    ps.setString(5, record.protocolDefinitionId);
                    ps.setString(6, record.actionType);
                    ps.setString(7, record.severity);
                    ps.setString(8, record.intelligenceDestination);
                    ps.setString(9, record.stepState);
                    ps.setString(10, record.actionId);
                    ps.setString(11, record.protocolCanonical);
                    ps.setTimestamp(12, Timestamp.from(record.detectedAt));
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
        )).name("clickhouse-intelligence-events-sink");

        // DLQ
        processed.getSideOutput(DLQ_TAG)
                .sinkTo(org.apache.flink.connector.kafka.sink.KafkaSink.<String>builder()
                        .setBootstrapServers(kafkaBootstrap)
                        .setRecordSerializer(
                                org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
                                        .setTopic("cce.intelligence.triggers.dlq")
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build()
                        )
                        .build())
                .name("dlq-sink");

        env.execute("CCE Intelligence Tracker");
    }

    public static class IntelligenceEventProcessor extends ProcessFunction<String, IntelligenceRecord> {
        @Override
        public void processElement(String value, Context ctx, Collector<IntelligenceRecord> out) {
            try {
                JsonNode root = MAPPER.readTree(value);
                IntelligenceRecord record = new IntelligenceRecord();
                record.id = uuidOrGenerate(root, "id");
                record.subject = textOrNull(root, "subject");
                record.intelligenceEventId = textOrNull(root, "intelligenceEventId");
                record.actionDefinitionId = textOrNull(root, "actionDefinitionId");
                record.protocolDefinitionId = textOrNull(root, "protocolDefinitionId");
                record.actionType = textOrNull(root, "actionType");
                record.severity = textOrNull(root, "severity");
                record.intelligenceDestination = textOrNull(root, "destination");
                record.stepState = textOrNull(root, "stepState");
                record.actionId = textOrNull(root, "actionId");
                record.protocolCanonical = textOrNull(root, "protocolCanonical");
                record.detectedAt = parseDetectedAt(root);
                out.collect(record);
            } catch (Exception e) {
                ctx.output(DLQ_TAG, value);
            }
        }

        private static String textOrNull(JsonNode node, String field) {
            JsonNode child = node.get(field);
            return child != null && !child.isNull() ? child.asText() : null;
        }

        private static String uuidOrGenerate(JsonNode node, String field) {
            String val = textOrNull(node, field);
            return val != null ? val : UUID.randomUUID().toString();
        }

        private static Instant parseDetectedAt(JsonNode root) {
            JsonNode detected = root.get("detectedAt");
            if (detected == null) return Instant.now();
            if (detected.isTextual()) {
                return Instant.parse(detected.asText());
            }
            if (detected.isNumber()) {
                return Instant.ofEpochMilli(detected.asLong());
            }
            return Instant.now();
        }
    }
}
