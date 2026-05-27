package org.openphc.cce.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

/**
 * Flink job: Enriches intelligence_delivery CDC events with adaptor information
 * by joining with destination_adaptor_mapping and receiver_adaptor CDC streams
 * using broadcast state.
 *
 * Extracts http_status_code, error_message from delivery_result JSONB.
 * Computes latency_ms = delivered_at - created_at.
 */
public class CdcEnrichmentJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Broadcast state: mapping_id -> {adaptor_name, endpoint_url}
    private static final MapStateDescriptor<String, String> ADAPTOR_STATE =
            new MapStateDescriptor<>("adaptor-lookup", Types.STRING, Types.STRING);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String clickhouseUrl = System.getenv().getOrDefault("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/cce_analytics");

        // Main stream: intelligence_delivery CDC events
        KafkaSource<String> deliverySource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("cce.cdc.public.intelligence_delivery")
                .setGroupId("cce-cdc-enrichment-delivery")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Broadcast stream: adaptor mapping + receiver_adaptor CDC events
        KafkaSource<String> adaptorSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("cce.cdc.public.destination_adaptor_mapping", "cce.cdc.public.receiver_adaptor")
                .setGroupId("cce-cdc-enrichment-adaptor")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> deliveries = env.fromSource(deliverySource, WatermarkStrategy.noWatermarks(), "delivery-source");
        DataStream<String> adaptors = env.fromSource(adaptorSource, WatermarkStrategy.noWatermarks(), "adaptor-source");

        BroadcastStream<String> adaptorBroadcast = adaptors.broadcast(ADAPTOR_STATE);

        DataStream<DeliveryRecord> enriched = deliveries
                .connect(adaptorBroadcast)
                .process(new DeliveryEnrichmentFunction());

        enriched.addSink(JdbcSink.sink(
                "INSERT INTO intelligence_deliveries (id, intelligence_event_id, action_definition_id, " +
                        "destination_adaptor_mapping_id, adaptor_name, endpoint_url, destination, action_type, " +
                        "severity, status, subject, protocol_canonical, action_id, http_status_code, " +
                        "error_message, attempt_count, created_at, delivered_at, latency_ms, _version) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, r) -> {
                    ps.setString(1, r.id);
                    ps.setString(2, r.intelligenceEventId);
                    ps.setString(3, r.actionDefinitionId);
                    ps.setString(4, r.destinationAdaptorMappingId);
                    ps.setString(5, r.adaptorName);
                    ps.setString(6, r.endpointUrl);
                    ps.setString(7, r.destination);
                    ps.setString(8, r.actionType);
                    ps.setString(9, r.severity);
                    ps.setString(10, r.status);
                    ps.setString(11, r.subject);
                    ps.setString(12, r.protocolCanonical);
                    ps.setString(13, r.actionId);
                    if (r.httpStatusCode != null) {
                        ps.setInt(14, r.httpStatusCode);
                    } else {
                        ps.setNull(14, java.sql.Types.INTEGER);
                    }
                    ps.setString(15, r.errorMessage);
                    ps.setInt(16, r.attemptCount);
                    ps.setTimestamp(17, r.createdAt != null ? Timestamp.from(r.createdAt) : null);
                    ps.setTimestamp(18, r.deliveredAt != null ? Timestamp.from(r.deliveredAt) : null);
                    if (r.latencyMs != null) {
                        ps.setLong(19, r.latencyMs);
                    } else {
                        ps.setNull(19, java.sql.Types.BIGINT);
                    }
                    ps.setLong(20, r.version);
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
        )).name("clickhouse-deliveries-sink");

        env.execute("CCE CDC Enrichment");
    }

    /**
     * Enriches delivery events using broadcast state of adaptor mappings.
     */
    private static class DeliveryEnrichmentFunction
            extends BroadcastProcessFunction<String, String, DeliveryRecord> {

        @Override
        public void processElement(String value, ReadOnlyContext ctx, Collector<DeliveryRecord> out) throws Exception {
            JsonNode root = MAPPER.readTree(value);
            DeliveryRecord record = new DeliveryRecord();

            record.id = textOrNull(root, "id");
            record.intelligenceEventId = textOrNull(root, "intelligence_event_id");
            record.actionDefinitionId = textOrNull(root, "action_definition_id");
            record.destinationAdaptorMappingId = textOrNull(root, "destination_adaptor_mapping_id");
            record.destination = textOrNull(root, "destination");
            record.actionType = textOrNull(root, "action_type");
            record.severity = textOrNull(root, "severity");
            record.status = textOrNull(root, "status");
            record.subject = textOrNull(root, "subject");
            record.protocolCanonical = textOrNull(root, "protocol_canonical");
            record.actionId = textOrNull(root, "action_id");
            record.attemptCount = root.has("attempt_count") ? root.get("attempt_count").asInt() : 0;
            record.version = root.has("__lsn") ? root.get("__lsn").asLong() :
                    (root.has("_version") ? root.get("_version").asLong() : System.currentTimeMillis());

            // Parse timestamps
            record.createdAt = parseTimestamp(root, "created_at");
            record.deliveredAt = parseTimestamp(root, "delivered_at");

            // Compute latency
            if (record.createdAt != null && record.deliveredAt != null) {
                record.latencyMs = Duration.between(record.createdAt, record.deliveredAt).toMillis();
                if (record.latencyMs < 0) record.latencyMs = null;
            }

            // Extract from delivery_result JSONB
            JsonNode deliveryResult = root.get("delivery_result");
            if (deliveryResult != null && deliveryResult.isTextual()) {
                try {
                    JsonNode result = MAPPER.readTree(deliveryResult.asText());
                    record.httpStatusCode = result.has("statusCode") ? result.get("statusCode").asInt() : null;
                    record.errorMessage = textOrNull(result, "error");
                } catch (Exception ignored) {}
            } else if (deliveryResult != null && deliveryResult.isObject()) {
                record.httpStatusCode = deliveryResult.has("statusCode") ? deliveryResult.get("statusCode").asInt() : null;
                record.errorMessage = textOrNull(deliveryResult, "error");
            }

            // Lookup adaptor info from broadcast state
            String mappingId = record.destinationAdaptorMappingId;
            if (mappingId != null) {
                String adaptorInfo = ctx.getBroadcastState(ADAPTOR_STATE).get(mappingId);
                if (adaptorInfo != null) {
                    String[] parts = adaptorInfo.split("\\|", 2);
                    record.adaptorName = parts[0];
                    record.endpointUrl = parts.length > 1 ? parts[1] : "";
                }
            }
            if (record.adaptorName == null) record.adaptorName = "unknown";
            if (record.endpointUrl == null) record.endpointUrl = "";

            out.collect(record);
        }

        @Override
        public void processBroadcastElement(String value, Context ctx, Collector<DeliveryRecord> out) throws Exception {
            JsonNode root = MAPPER.readTree(value);

            // Determine if this is a destination_adaptor_mapping or receiver_adaptor event
            if (root.has("receiver_adaptor_id")) {
                // destination_adaptor_mapping: store mapping_id -> receiver_adaptor_id
                String mappingId = textOrNull(root, "id");
                String receiverAdaptorId = textOrNull(root, "receiver_adaptor_id");
                if (mappingId != null && receiverAdaptorId != null) {
                    // Temporarily store as mapping_id -> receiver_adaptor_id (will be resolved)
                    ctx.getBroadcastState(ADAPTOR_STATE).put(mappingId, "pending|" + receiverAdaptorId);
                }
            } else if (root.has("name") && root.has("definition")) {
                // receiver_adaptor: update all mappings that reference this adaptor
                String adaptorId = textOrNull(root, "id");
                String name = textOrNull(root, "name");
                String endpointUrl = "";
                JsonNode definition = root.get("definition");
                if (definition != null) {
                    if (definition.isTextual()) {
                        try {
                            JsonNode defJson = MAPPER.readTree(definition.asText());
                            endpointUrl = textOrNull(defJson, "address");
                        } catch (Exception ignored) {}
                    } else if (definition.isObject()) {
                        endpointUrl = textOrNull(definition, "address");
                    }
                }
                if (endpointUrl == null) endpointUrl = "";

                // Store adaptor info: adaptorId -> name|endpoint
                String adaptorInfo = name + "|" + endpointUrl;
                ctx.getBroadcastState(ADAPTOR_STATE).put("adaptor:" + adaptorId, adaptorInfo);

                // Resolve any pending mappings
                for (var entry : ctx.getBroadcastState(ADAPTOR_STATE).immutableEntries()) {
                    if (entry.getValue().startsWith("pending|" + adaptorId)) {
                        ctx.getBroadcastState(ADAPTOR_STATE).put(entry.getKey(), adaptorInfo);
                    }
                }
            }
        }

        private static String textOrNull(JsonNode node, String field) {
            JsonNode child = node.get(field);
            return child != null && !child.isNull() && child.isTextual() ? child.asText() : null;
        }

        private static Instant parseTimestamp(JsonNode node, String field) {
            JsonNode child = node.get(field);
            if (child == null || child.isNull()) return null;
            try {
                if (child.isTextual()) return Instant.parse(child.asText());
                if (child.isLong()) return Instant.ofEpochMilli(child.asLong());
            } catch (Exception ignored) {}
            return null;
        }
    }
}
