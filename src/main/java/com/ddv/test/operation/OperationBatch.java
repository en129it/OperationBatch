package com.ddv.test.operation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class OperationBatch {

	private static long BATCH_SEQUENCE_ID;
	
	private long id;
	private OperationExecutorConfiguration config;
	private OperationExecutorContext context;
	private List<Operation> operations;
	private OperationBatchExecutionStatus status;
	private long creationTimestampInMsec;
	private Long lastExecutionTimestampInMsec;
	
	public OperationBatch(OperationExecutorConfiguration config, OperationExecutorContext context, Operation operation) {
		this.config = config;
		this.context = context;
		id = generateNextBatchId();
		operations = new ArrayList<Operation>(config.getBatchCapacity());
		status = OperationBatchExecutionStatus.WARMING;		
		
		addOperation(operation);
		creationTimestampInMsec = System.currentTimeMillis();
		scheduleBatchExecution(config.getOperationMaxDelayExecutionInMsec());
	}
	
	public long getId() {
		return id;
	}
	
	public long getCreationTimestampInMsec() {
		return creationTimestampInMsec;
	}
	
	public Long getLastExecutionTimestampInMsec() {
		return lastExecutionTimestampInMsec;
	}
	
	public boolean isExecutionSucceeded() {
		return OperationBatchExecutionStatus.SUCCEEDED.equals(status);
	}
	
	public synchronized Operation getOperationForBusinessId(long businessId) {
		for (Operation operation : operations) {
			if (Objects.equals(operation.getBusinessId(), businessId)) {
				return operation;
			}
		}
		return null;
	}
	
	public synchronized Operation getOperationById(long operationId) {
		for (Operation operation : operations) {
			if (Objects.equals(operation.getOperationId(), operationId)) {
				return operation;
			}
		}
		return null;
	}
	
	private synchronized boolean isAcceptingNewOperation() {
		return (operations.size() != config.getBatchCapacity()) && (OperationBatchExecutionStatus.WARMING.equals(status));
	}
	
	public synchronized boolean addOperation(Operation operation) {
		if (isAcceptingNewOperation()) {
			operations.add(operation);
			if (!isAcceptingNewOperation()) {
				execute();
			}
			return true;
		}
		return false;
	}
	
	private void scheduleBatchExecution(long delay) {
		context.getScheduledExecutorService().schedule(new Runnable() {
			public void run() {
				OperationBatch.this.execute();
			}
		}, delay, TimeUnit.MILLISECONDS);
	}
	
	private void execute() {
		synchronized(this) {
			if (OperationBatchExecutionStatus.WARMING.equals(status) || OperationBatchExecutionStatus.FAILED.equals(status) || OperationBatchExecutionStatus.WAITING_OTHER_EXECUTION_END.equals(status)) {
				status = OperationBatchExecutionStatus.EXECUTING;
				context.getExecutorService().execute(new Runnable() {
					public void run() {
						if (!canExecute()) {
							status = OperationBatchExecutionStatus.WAITING_OTHER_EXECUTION_END;
							return;
						}
						try {							
							Connection connection = context.getDataSource().getConnection();
							PreparedStatement pstmt = connection.prepareStatement("");
							for (Operation operation : operations) {
								pstmt.addBatch();
							}
							pstmt.executeBatch();
							status = OperationBatchExecutionStatus.SUCCEEDED;
							
							Set<OperationBatch> toWakeUpBatchSet = new HashSet<>();
							OperationBatch batch = null;
							for (Operation operation : operations) {
								operation.onComplete();
								
								if (operation.getDependsOnOperationId() != null) {
									batch = context.getOperationBatchByOperationId(operation.getDependsOnOperationId());
									if (batch != null) {
										toWakeUpBatchSet.add(batch);
									}
								}
							}
							toWakeUpBatchSet.forEach(OperationBatch::execute);
						} catch (Exception ex) {
							status = OperationBatchExecutionStatus.FAILED;
							scheduleBatchExecution(config.getOperationMaxDelayExecutionInMsec());							
						} finally {
							lastExecutionTimestampInMsec = System.currentTimeMillis();
						}
					}
					
					private boolean canExecute() {
						for (Operation operation : operations) {
							if (context.containsWaitingToExecuteOperation(operation.getDependsOnOperationId())) {
								return false;
							}
						}
						return true;
					}
				});
			}
		}
	}
	
	//**************************************************************************
	
	public static final long generateNextBatchId() {
		synchronized(OperationBatch.class) {
			BATCH_SEQUENCE_ID += 1;
			return BATCH_SEQUENCE_ID;
		}
	}
	
	public static enum OperationBatchExecutionStatus {
		WARMING,
		WAITING_OTHER_EXECUTION_END,
		EXECUTING,
		SUCCEEDED,
		FAILED
	}
	
	
}
