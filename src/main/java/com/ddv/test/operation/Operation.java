package com.ddv.test.operation;

import java.util.function.Consumer;

public class Operation {
	private static long OPERATION_ID = 0;
	
	private OperationTypeEnum operationType;
	private long operationId;
	private long businessId;
	private PartitionOffset offset;
	private Long dependsOnOperationId;
	private Consumer<PartitionOffset> callback;
	
	public Operation(OperationTypeEnum operationType, long businessId, PartitionOffset offset, Consumer<PartitionOffset> callback) {
		this.operationType = operationType;
		this.businessId = businessId;
		this.offset = offset;
		this.callback = callback;
		operationId = generateNextOperationId();
	}
	
	public long getOperationId() {
		return operationId;
	}
	
	public OperationTypeEnum getOperationType() {
		return operationType;
	}

	public Long getDependsOnOperationId() {
		return dependsOnOperationId;
	}
	
	public void setDependsOnOperationId(long dependsOnOperationId) {
		this.dependsOnOperationId = dependsOnOperationId;
	}
	
	public long getBusinessId() {
		return businessId;
	}
	
	private static final long generateNextOperationId() {
		synchronized(Operation.class) {
			OPERATION_ID += 1;
			return OPERATION_ID;
		}
	}
	
	void onComplete() {
		if (callback != null) {
			callback.accept(offset);
		}
	}
}
