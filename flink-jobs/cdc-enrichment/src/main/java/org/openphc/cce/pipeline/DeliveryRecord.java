package org.openphc.cce.pipeline;

import java.io.Serializable;
import java.time.Instant;

public class DeliveryRecord implements Serializable {
    public String id;
    public String intelligenceEventId;
    public String actionDefinitionId;
    public String destinationAdaptorMappingId;
    public String adaptorName;
    public String endpointUrl;
    public String destination;
    public String actionType;
    public String severity;
    public String status;
    public String subject;
    public String protocolCanonical;
    public String actionId;
    public Integer httpStatusCode;
    public String errorMessage;
    public int attemptCount;
    public Instant createdAt;
    public Instant deliveredAt;
    public Long latencyMs;
    public long version;
}
