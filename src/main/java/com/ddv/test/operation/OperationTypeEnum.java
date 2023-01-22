package com.ddv.test.operation;

public enum OperationTypeEnum {

	INSERT_TYPE1(null),
	INSERT_TYPE2(null),
	UPDATE_TYPE1(INSERT_TYPE1),
	UPDATE_TYPE2(INSERT_TYPE2);
	
	private OperationTypeEnum dependsOn;
	
	private OperationTypeEnum(OperationTypeEnum dependsOn) {
		this.dependsOn = dependsOn;
	}
	
	public OperationTypeEnum getDependsOn() {
		return dependsOn;
	}
}
