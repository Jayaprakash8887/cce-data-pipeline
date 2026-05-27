package org.openphc.cce.pipeline;

import java.io.Serializable;
import java.time.Instant;

/**
 * POJO representing an enriched event record for the events_fact table.
 */
public class EventRecord implements Serializable {
    public String eventId;
    public String source;
    public String eventType;
    public String patientId;
    public Instant eventTime;
    public String facilityId;
    public String correlationId;
    public String contentType;
    public String resourceType;
    public String resourceStatus;
    public String primaryCodeSystem;
    public String primaryCode;
    public String primaryCodeDisplay;
    public String practitionerRef;
    public String rawPayload;
}
