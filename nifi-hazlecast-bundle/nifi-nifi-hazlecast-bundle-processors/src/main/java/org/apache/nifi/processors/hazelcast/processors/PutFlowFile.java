package org.apache.nifi.processors.hazelcast.processors;

import org.apache.nifi.flowfile.FlowFile;

public class PutFlowFile {
    private FlowFile flowFile;
    private Boolean isSuccess;

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public void setFlowFile(FlowFile flowFile) {
        this.flowFile = flowFile;
    }

    public Boolean getSuccess() {
        return isSuccess;
    }

    public void setSuccess(Boolean success) {
        isSuccess = success;
    }
}
