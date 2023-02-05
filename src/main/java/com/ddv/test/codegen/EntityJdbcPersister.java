package com.ddv.test.codegen;

import java.util.List;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

public interface EntityJdbcPersister<T> {

	public String getInsertSql();

	public BatchPreparedStatementSetter getBatchPreparedStatementSetter(List entities);
	
	public RowMapper<T> getRowMapper();
	
	/*	
	public String getUpdateSql();
	
	
*/	
	public String sayHello();
}
