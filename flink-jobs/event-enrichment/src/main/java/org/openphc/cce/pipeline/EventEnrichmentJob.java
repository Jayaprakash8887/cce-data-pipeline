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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * Flink job: Reads CloudEvents from cce.events.inbound, extracts FHIR fields,
 * and sinks enriched records to ClickHouse events_fact table.
 */
public class EventEnrichmentJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final OutputTag<String> DLQ_TAG = new OutputTag<>("dlq") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String clickhouseUrl = System.getenv().getOrDefault("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/cce_analytics");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("cce.events.inbound")
                .setGroupId("cce-event-enrichment")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        SingleOutputStreamOperator<EventRecord> enriched = kafkaStream
                .process(new CloudEventProcessor());

        // Main sink to ClickHouse events_fact
        enriched.addSink(JdbcSink.sink(
                "INSERT INTO events_fact (event_id, source, event_type, patient_id, event_time, " +
                        "facility_id, correlation_id, content_type, resource_type, resource_status, " +
                        "primary_code_system, primary_code, primary_code_display, practitioner_ref, practitioner_display, raw_payload) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, record) -> {
                    ps.setString(1, record.eventId);
                    ps.setString(2, record.source);
                    ps.setString(3, record.eventType);
                    ps.setString(4, record.patientId);
                    ps.setTimestamp(5, Timestamp.from(record.eventTime));
                    ps.setString(6, record.facilityId);
                    ps.setString(7, record.correlationId);
                    ps.setString(8, record.contentType);
                    ps.setString(9, record.resourceType);
                    ps.setString(10, record.resourceStatus);
                    ps.setString(11, record.primaryCodeSystem);
                    ps.setString(12, record.primaryCode);
                    ps.setString(13, record.primaryCodeDisplay);
                    ps.setString(14, record.practitionerRef);
                    ps.setString(15, record.practitionerDisplay);
                    ps.setString(16, record.rawPayload);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(clickhouseUrl)
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .build()
        )).name("clickhouse-events-fact-sink");

        // DLQ sink (invalid events sent back to Kafka DLQ topic)
        enriched.getSideOutput(DLQ_TAG)
                .sinkTo(org.apache.flink.connector.kafka.sink.KafkaSink.<String>builder()
                        .setBootstrapServers(kafkaBootstrap)
                        .setRecordSerializer(
                                org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
                                        .setTopic("cce.events.inbound.dlq")
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build()
                        )
                        .build())
                .name("dlq-sink");

        env.execute("CCE Event Enrichment");
    }

    /**
     * Processes raw CloudEvent JSON, extracts fields, routes invalid events to DLQ.
     */
    public static class CloudEventProcessor extends ProcessFunction<String, EventRecord> {

        @Override
        public void processElement(String value, Context ctx, Collector<EventRecord> out) {
            try {
                JsonNode root = MAPPER.readTree(value);

                String patientId = textOrNull(root, "subject");
                if (patientId == null || patientId.isBlank()) {
                    ctx.output(DLQ_TAG, value);
                    return;
                }

                Instant eventTime = parseTime(textOrNull(root, "time"));
                if (eventTime == null || eventTime.isAfter(Instant.now().plusSeconds(300))) {
                    ctx.output(DLQ_TAG, value);
                    return;
                }

                JsonNode data = root.get("data");
                EventRecord record = new EventRecord();
                record.eventId = textOrNull(root, "id");
                record.source = textOrNull(root, "source");
                record.eventType = textOrNull(root, "type");
                record.patientId = patientId;
                record.eventTime = eventTime;
                record.facilityId = textOrNull(root, "facilityid");
                record.correlationId = textOrNull(root, "correlationid");
                record.contentType = textOrNull(root, "datacontenttype");

                if (data != null) {
                    record.resourceType = textOrNull(data, "resourceType");
                    record.resourceStatus = textOrNull(data, "status");
                    record.primaryCodeSystem = extractCodeField(data, "system");
                    record.primaryCode = extractCodeField(data, "code");
                    record.primaryCodeDisplay = extractCodeField(data, "display");
                    record.practitionerRef = extractPractitioner(data);
                    record.practitionerDisplay = extractPractitionerDisplay(data);
                }

                record.rawPayload = data != null ? data.toString() : "";
                out.collect(record);
            } catch (Exception e) {
                ctx.output(DLQ_TAG, value);
            }
        }

        private String extractCodeField(JsonNode data, String field) {
            JsonNode code = data.path("code").path("coding");
            if (code.isArray() && !code.isEmpty()) {
                return textOrNull(code.get(0), field);
            }
            return null;
        }

        /**
         * COALESCE practitioner reference from multiple FHIR paths.
         */
        static String extractPractitioner(JsonNode data) {
            String ref;
            // participant[0].individual.reference
            ref = textAtPath(data, "participant", 0, "individual", "reference");
            if (ref != null) return ref;
            // performer[0].reference
            ref = textAtPath(data, "performer", 0, "reference");
            if (ref != null) return ref;
            // asserter.reference
            ref = textAtPath(data, "asserter", "reference");
            if (ref != null) return ref;
            // requester.reference
            ref = textAtPath(data, "requester", "reference");
            if (ref != null) return ref;
            // performer[0].actor.reference
            ref = textAtPath(data, "performer", 0, "actor", "reference");
            return ref;
        }

        /**
         * Extract practitioner display name from FHIR paths (mirrors extractPractitioner paths).
         */
        static String extractPractitionerDisplay(JsonNode data) {
            String display;
            display = textAtPath(data, "participant", 0, "individual", "display");
            if (display != null) return display;
            display = textAtPath(data, "performer", 0, "display");
            if (display != null) return display;
            display = textAtPath(data, "asserter", "display");
            if (display != null) return display;
            display = textAtPath(data, "requester", "display");
            if (display != null) return display;
            display = textAtPath(data, "performer", 0, "actor", "display");
            return display;
        }

        private static String textAtPath(JsonNode node, Object... path) {
            JsonNode current = node;
            for (Object segment : path) {
                if (current == null || current.isMissingNode()) return null;
                if (segment instanceof Integer idx) {
                    current = current.isArray() && current.size() > idx ? current.get(idx) : null;
                } else {
                    current = current.get((String) segment);
                }
            }
            return current != null && current.isTextual() ? current.asText() : null;
        }

        private static String textOrNull(JsonNode node, String field) {
            JsonNode child = node.get(field);
            return child != null && child.isTextual() ? child.asText() : null;
        }

        private static Instant parseTime(String time) {
            if (time == null) return null;
            try {
                return Instant.parse(time);
            } catch (Exception e) {
                return null;
            }
        }
    }
}
