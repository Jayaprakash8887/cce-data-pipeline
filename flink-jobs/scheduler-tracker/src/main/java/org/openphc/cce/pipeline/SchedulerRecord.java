package org.openphc.cce.pipeline;

import java.io.Serializable;
import java.time.Instant;

public class SchedulerRecord implements Serializable {
    public String stepInstanceId;
    public String transitionType;
    public Instant triggeredAt;
    public String correlationId;
}
