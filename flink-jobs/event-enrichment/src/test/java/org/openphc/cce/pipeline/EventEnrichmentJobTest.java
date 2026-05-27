package org.openphc.cce.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventEnrichmentJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void extractPractitioner_fromParticipant() throws Exception {
        String json = """
            {
                "resourceType": "Encounter",
                "participant": [{"individual": {"reference": "Practitioner/123"}}]
            }
            """;
        JsonNode data = MAPPER.readTree(json);
        String ref = EventEnrichmentJob.CloudEventProcessor.extractPractitioner(data);
        assertThat(ref).isEqualTo("Practitioner/123");
    }

    @Test
    void extractPractitioner_fromPerformer() throws Exception {
        String json = """
            {
                "resourceType": "Observation",
                "performer": [{"reference": "Practitioner/456"}]
            }
            """;
        JsonNode data = MAPPER.readTree(json);
        String ref = EventEnrichmentJob.CloudEventProcessor.extractPractitioner(data);
        assertThat(ref).isEqualTo("Practitioner/456");
    }

    @Test
    void extractPractitioner_fromAsserter() throws Exception {
        String json = """
            {
                "resourceType": "Condition",
                "asserter": {"reference": "Practitioner/789"}
            }
            """;
        JsonNode data = MAPPER.readTree(json);
        String ref = EventEnrichmentJob.CloudEventProcessor.extractPractitioner(data);
        assertThat(ref).isEqualTo("Practitioner/789");
    }

    @Test
    void extractPractitioner_fromRequester() throws Exception {
        String json = """
            {
                "resourceType": "ServiceRequest",
                "requester": {"reference": "Practitioner/321"}
            }
            """;
        JsonNode data = MAPPER.readTree(json);
        String ref = EventEnrichmentJob.CloudEventProcessor.extractPractitioner(data);
        assertThat(ref).isEqualTo("Practitioner/321");
    }

    @Test
    void extractPractitioner_fromPerformerActor() throws Exception {
        String json = """
            {
                "resourceType": "Task",
                "performer": [{"actor": {"reference": "Practitioner/actor1"}}]
            }
            """;
        JsonNode data = MAPPER.readTree(json);
        String ref = EventEnrichmentJob.CloudEventProcessor.extractPractitioner(data);
        assertThat(ref).isEqualTo("Practitioner/actor1");
    }

    @Test
    void extractPractitioner_returnsNull_whenNoPractitioner() throws Exception {
        String json = """
            {
                "resourceType": "Patient",
                "name": [{"family": "Smith"}]
            }
            """;
        JsonNode data = MAPPER.readTree(json);
        String ref = EventEnrichmentJob.CloudEventProcessor.extractPractitioner(data);
        assertThat(ref).isNull();
    }
}
