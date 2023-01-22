package com.ddv.test.operation;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import javax.sql.DataSource;

public abstract class OperationExecutorContext {

	private ExecutorService executorService;
	private ScheduledExecutorService scheduledExecutorService;
	private DataSource dataSource;
	
	public OperationExecutorContext(ExecutorService executorService, ScheduledExecutorService scheduledExecutorService, DataSource dataSource) {
		this.executorService = executorService;
		this.scheduledExecutorService = scheduledExecutorService;
		this.dataSource = dataSource;
	}
	
	public ExecutorService getExecutorService() {
		return executorService;
	}
	public ScheduledExecutorService getScheduledExecutorService() {
		return scheduledExecutorService;
	}
	public DataSource getDataSource() {
		return dataSource;
	}
	
	public abstract boolean containsWaitingToExecuteOperation(Long operationId);
	
	public abstract OperationBatch getOperationBatchByOperationId(long operationId);
}
