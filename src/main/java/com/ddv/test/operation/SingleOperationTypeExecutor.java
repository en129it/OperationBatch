package com.ddv.test.operation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class SingleOperationTypeExecutor {

	private OperationExecutorConfiguration config;
	private OperationExecutorContext context;
	private List<OperationBatch> batches;
	
	public SingleOperationTypeExecutor(OperationExecutorConfiguration config, OperationExecutorContext context) {
		this.config = config;
		this.context = context;
		batches = new ArrayList<OperationBatch>();
	}
	
	public synchronized void addOperation(Operation operation) {
		OperationBatch operationBatch = null;
		
		operationBatch = scanBatchAndRemoveSucceeded(b -> {
			return (b.addOperation(operation)) ? b : null;
		});
		
		if ((operationBatch == null) && (batches.size() < config.getMaxBatches())) {
			operationBatch = new OperationBatch(config, context, operation);
			batches.add(operationBatch);
		} else {
			throw new RuntimeException("Maximum operation capacity reached");
		}
	}
	
	public Operation getOperationForBusinessId(final long businessId) {
		return scanBatchAndRemoveSucceeded(batch -> batch.getOperationForBusinessId(businessId));
	}
	
	public Operation getOperationById(final long operationId) {
		return scanBatchAndRemoveSucceeded(batch -> batch.getOperationById(operationId));
	}
	
	public OperationBatch getOperationBatchByOperationId(long operationId) {
		Iterator<OperationBatch> iter = batches.iterator();
		while (iter.hasNext()) {
			OperationBatch batch = iter.next();
			if (batch.getOperationById(operationId) != null) {
				return batch;
			}
		}
		return null;
	}
	
	private <T> T scanBatchAndRemoveSucceeded(Function<OperationBatch, T> scanner) {
		T rslt = null;
		
		Iterator<OperationBatch> iter = batches.iterator();
		while (iter.hasNext()) {
			OperationBatch batch = iter.next();
			if (batch.isExecutionSucceeded()) {
				iter.remove();
			} else {
				rslt = scanner.apply(batch);
				if (rslt != null) {
					return rslt;
				}
			}
		}
		return null;
	}
}
