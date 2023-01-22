package com.ddv.test.operation;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

public class MultiOperationTypeExecutor {

	private SingleOperationTypeExecutor[] executors;
	private OperationExecutorContext context = null;
	
	public MultiOperationTypeExecutor() {
		int operationTypeCount = OperationTypeEnum.values().length;
		
		int dependsOnCount = 0;
		for (int i = 0; i < operationTypeCount; i++) {
			if (OperationTypeEnum.values()[i].getDependsOn() != null) {
				dependsOnCount++;
			};
		}
		ExecutorService executorService = Executors.newFixedThreadPool( (operationTypeCount - dependsOnCount) + (dependsOnCount / 2));
		ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
		context = new OperationExecutorContext(executorService, scheduledExecutorService, null) {
			@Override
			public boolean containsWaitingToExecuteOperation(Long operationId) {
				return (operationId != null) ? (iterateExecutors( s -> s.getOperationById(operationId)) != null) : null;
			}
			
			@Override
			public OperationBatch getOperationBatchByOperationId(long operationId) {
				return iterateExecutors( s -> s.getOperationBatchByOperationId(operationId));
			}
		};
		
		executors = new SingleOperationTypeExecutor[operationTypeCount];
		
		for (int i = 0; i < operationTypeCount; i++) {
			executors[i] = new SingleOperationTypeExecutor(new OperationExecutorConfiguration(10, 2000, 5, 30000), context);
		}		
	}
	
	public void addOperation(Operation operation) {
		executors[operation.getOperationType().ordinal()].addOperation(operation);
	}
	
	private <T> T iterateExecutors(Function<SingleOperationTypeExecutor, T> selectorFct) {
		T rslt = null;
		for (int i = 0; i < executors.length; i++) {
			rslt = selectorFct.apply(executors[i]);
			if (rslt != null) {
				return rslt;
			}
		}
		return null;
	}
	
}
