package net.fhirfactory.pegacorn.ponos.workshops.datagrid.persistence.datatypes;

import com.fasterxml.jackson.annotation.JsonFormat;
import net.fhirfactory.pegacorn.core.constants.petasos.PetasosPropertyConstants;
import net.fhirfactory.pegacorn.core.model.petasos.task.datatypes.status.valuesets.TaskOutcomeStatusEnum;

import java.io.Serializable;
import java.time.Instant;

public class BatchTaskRetrievalRequest implements Serializable {
    private String rangeStartToken;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSSXXX", timezone = PetasosPropertyConstants.DEFAULT_TIMEZONE)
    private Instant rangeLowerBound;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSSXXX", timezone = PetasosPropertyConstants.DEFAULT_TIMEZONE)
    private Instant rangeUpperBound;
    private Integer count;
    private TaskOutcomeStatusEnum taskStatus;

    //
    // Constructor(s)
    //

    public BatchTaskRetrievalRequest(){
        this.rangeStartToken = null;
        this.rangeUpperBound = null;
        this.rangeLowerBound = null;
        this.count = 0;
        this.taskStatus = null;
    }

    //
    // Getters and Setters
    //

    public String getRangeStartToken() {
        return rangeStartToken;
    }

    public void setRangeStartToken(String rangeStartToken) {
        this.rangeStartToken = rangeStartToken;
    }

    public Instant getRangeLowerBound() {
        return rangeLowerBound;
    }

    public void setRangeLowerBound(Instant rangeLowerBound) {
        this.rangeLowerBound = rangeLowerBound;
    }

    public Instant getRangeUpperBound() {
        return rangeUpperBound;
    }

    public void setRangeUpperBound(Instant rangeUpperBound) {
        this.rangeUpperBound = rangeUpperBound;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public TaskOutcomeStatusEnum getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(TaskOutcomeStatusEnum taskStatus) {
        this.taskStatus = taskStatus;
    }

    //
    // toString()
    //

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TaskRetrievalRequest{");
        sb.append("rangeStartToken='").append(rangeStartToken).append('\'');
        sb.append(", rangeLowerBound=").append(rangeLowerBound);
        sb.append(", rangeUpperBound=").append(rangeUpperBound);
        sb.append(", count=").append(count);
        sb.append(", taskStatus=").append(taskStatus);
        sb.append('}');
        return sb.toString();
    }
}
