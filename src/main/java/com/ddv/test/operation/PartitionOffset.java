package com.ddv.test.operation;

public class PartitionOffset {

	private long offset;
	private int partition;
	
	public PartitionOffset(long offset, int partition) {
		this.offset = offset;
		this.partition = partition;
	}

	public long getOffset() {
		return offset;
	}

	public int getPartition() {
		return partition;
	}
	
}
