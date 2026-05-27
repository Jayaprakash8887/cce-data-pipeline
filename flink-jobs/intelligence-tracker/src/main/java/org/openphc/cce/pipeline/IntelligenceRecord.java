package org.openphc.cce.pipeline;

import java.io.Serializable;
import java.time.Instant;

public class IntelligenceRecord implements Serializable {
    public String id;
    public String subject;
    public String intelligenceEventId;
    public String actionDefinitionId;
    public String protocolDefinitionId;
    public String actionType;
    public String severity;
    public String intelligenceDestination;
    public String stepState;
    public String actionId;
    public String protocolCanonical;
    public Instant detectedAt;
}
