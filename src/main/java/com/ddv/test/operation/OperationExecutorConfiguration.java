package com.ddv.test.operation;

public class OperationExecutorConfiguration {

	private int batchCapacity;
	private long operationMaxDelayExecutionInMsec;
	private int maxBatches;
	private long batchExecutionRetrialTimeIntervalInMsec;
	
	public OperationExecutorConfiguration(int batchCapacity, long operationMaxDelayExecutionInMsec, int maxBatches, long batchExecutionRetrialTimeIntervalInMsec) {
		this.batchCapacity = batchCapacity;
		this.operationMaxDelayExecutionInMsec = operationMaxDelayExecutionInMsec;
		this.maxBatches = maxBatches;
		this.batchExecutionRetrialTimeIntervalInMsec = batchExecutionRetrialTimeIntervalInMsec;
	}
	
	public int getBatchCapacity() {
		return batchCapacity;
	}
	public long getOperationMaxDelayExecutionInMsec() {
		return operationMaxDelayExecutionInMsec;
	}
	public int getMaxBatches() {
		return maxBatches;
	}
	public long getBatchExecutionRetrialTimeIntervalInMsec() {
		return batchExecutionRetrialTimeIntervalInMsec;
	}
	
}
