package com.ddv.test.codegen;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import javax.persistence.Column;
import javax.persistence.Table;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewMethod;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class EntityJdbcPersisterFactory {

	private static final Map<Class, DataTypeMetadata> DATA_TYPE_MAPPER = new HashMap<>();
	static {
		DATA_TYPE_MAPPER.put(Long.class, new DataTypeMetadata(Long.class, "Long", Types.NUMERIC, "longValue", Long.TYPE));
		DATA_TYPE_MAPPER.put(Integer.class, new DataTypeMetadata(Integer.class, "Int", Types.NUMERIC, "intValue", Integer.TYPE));
		DATA_TYPE_MAPPER.put(String.class, new DataTypeMetadata(String.class, "String", Types.VARCHAR, null, null));
	}
	
	@SuppressWarnings("unchecked")
	public <T> EntityJdbcPersister<T> generateJdbcPersister(Class<T> entityType) throws Exception {
		String tableName = extractEntityTableName(entityType);
		EntityColumnMetaData[] columnMetadatas = extractEntityColumns(entityType);
		
		
		ClassPool pool = ClassPool.getDefault();
		
		String entityListGlobalVariableName = "entities";
		String entitySetterName = "setEntities";
		CtClass batchPreparedStatementSetterCtClass = pool.makeClass(EntityJdbcPersister.class.getPackage().getName() + "." + BatchPreparedStatementSetter.class.getSimpleName() + entityType.getSimpleName());
		batchPreparedStatementSetterCtClass.addInterface(pool.get(BatchPreparedStatementSetter.class.getName()));
		CtField entityListField = CtField.make("private java.util.List " + entityListGlobalVariableName + ";", batchPreparedStatementSetterCtClass);
		batchPreparedStatementSetterCtClass.addField(entityListField);
		batchPreparedStatementSetterCtClass.addMethod(CtNewMethod.make(generateBatchPreparedStatementSetterSetEntities(batchPreparedStatementSetterCtClass, entitySetterName, entityListGlobalVariableName), batchPreparedStatementSetterCtClass));
		batchPreparedStatementSetterCtClass.addMethod(CtNewMethod.make(generateBatchPreparedStatementSetterGetBatchSize(batchPreparedStatementSetterCtClass, entityListGlobalVariableName), batchPreparedStatementSetterCtClass));
		batchPreparedStatementSetterCtClass.addMethod(CtNewMethod.make(generateBatchPreparedStatementSetterSetValues(batchPreparedStatementSetterCtClass, entityListGlobalVariableName, entityType, columnMetadatas), batchPreparedStatementSetterCtClass));
		Class batchPreparedStatementSetterClass = batchPreparedStatementSetterCtClass.toClass();
		

		CtClass rowMapperCtClass = pool.makeClass(EntityJdbcPersister.class.getPackage().getName() + "." + RowMapper.class.getSimpleName() + entityType.getSimpleName());
		rowMapperCtClass.addInterface(pool.get(RowMapper.class.getName()));
		rowMapperCtClass.addMethod(CtNewMethod.make(generateRowMapperMapRow(rowMapperCtClass, entityType, columnMetadatas), rowMapperCtClass));
		Class rowMapperClass = rowMapperCtClass.toClass();

		
		
		CtClass entityJdbcPersisterClass = pool.makeClass(EntityJdbcPersister.class.getPackage().getName() + "." + entityType.getSimpleName() + "Pesister");

		entityJdbcPersisterClass.addInterface(pool.get(EntityJdbcPersister.class.getName()));

		entityJdbcPersisterClass.addMethod(CtNewMethod.make(generateGetInsertSql(tableName, columnMetadatas), entityJdbcPersisterClass));
		entityJdbcPersisterClass.addMethod(CtNewMethod.make(generateGetBatchPreparedStatementSetter(batchPreparedStatementSetterClass, entitySetterName), entityJdbcPersisterClass));
		entityJdbcPersisterClass.addMethod(CtNewMethod.make(generateGetRowMapper(rowMapperClass), entityJdbcPersisterClass));
		
		return (EntityJdbcPersister<T>)entityJdbcPersisterClass.toClass().newInstance();
	}
	
	private void generateResultSetGetter(int columnIndex, EntityColumnMetaData columnMetadata, String resultSetVariableName, String entityVariableName, Map<Class, DataTypeMetadata> dataTypeMetadata, StringBuilder out) {
		String fieldVariableName = "f" + columnIndex;
		
		DataTypeMetadata metadata = dataTypeMetadata.get(columnMetadata.getDataType());
		
		if (metadata.getPrimitiveType() != null) {
			out.append(metadata.getPrimitiveType()).append(" ").append(fieldVariableName).append(" = ").append(resultSetVariableName).append(".get").append(metadata.getPropertyName()).append("(").append(columnIndex).append(");\n");
			out.append("if (!").append(resultSetVariableName).append(".wasNull()) {\n")
				.append(entityVariableName).append(".").append(columnMetadata.getSetterMethodName()).append("(new ").append(metadata.getType().getName()).append("(").append(fieldVariableName).append("));\n")
			.append("}\n");
		} else {
			CodeBuilderHelper.buildCallVariableMethod(entityVariableName, columnMetadata.getSetterMethodName(), 
					b -> { b.append(resultSetVariableName).append(".get").append(metadata.getPropertyName()).append("(").append(columnIndex).append(")"); }, 
					out);
		}		
	}

	private <T> String generateRowMapperMapRow(CtClass rowMapperCtClass, Class<T> entityType, EntityColumnMetaData[] columnMetadatas) {
		String resultSetArgumentName = "rs";
		String entityVariableName = "entity";
		
		StringBuilder rslt = new StringBuilder()
		.append("public ").append(entityType.getName()).append(" mapRow(java.sql.ResultSet ").append(resultSetArgumentName).append(", int rowNum) throws java.sql.SQLException {\n");

		CodeBuilderHelper.buildVariableDeclarationAndAssignment(entityType, entityVariableName, 
				b -> { CodeBuilderHelper.buildEmptyConstructor(entityType, b); },
				rslt);
		
		for (int i = 0; i < columnMetadatas.length; i++) {
			generateResultSetGetter(i + 1, columnMetadatas[i], resultSetArgumentName, entityVariableName, DATA_TYPE_MAPPER, rslt);
		}
		CodeBuilderHelper.buildMethodReturn(b -> {b.append(entityVariableName); }, rslt);
		CodeBuilderHelper.buildCloseMethod(rslt);
		System.out.println("#### G: " + rslt.toString());
		return rslt.toString();
	}
	
	private String generateBatchPreparedStatementSetterSetEntities(CtClass batchPreparedStatementSetterClass, String entitySetterName, String entityListGlobalVariableName) {
		String entitiesArgumentName = "entities";
		StringBuilder rslt = new StringBuilder()
		.append("public void ").append(entitySetterName).append("(").append(List.class.getName()).append(" ").append(entitiesArgumentName).append(") {\n");
		CodeBuilderHelper.buildThisFieldAssignment(entityListGlobalVariableName, b-> { b.append(entitiesArgumentName).append(";\n"); }, rslt);
		CodeBuilderHelper.buildCloseMethod(rslt);
		System.out.println("#### A: " + rslt.toString());
		return rslt.toString();
	}

	private String generateBatchPreparedStatementSetterGetBatchSize(CtClass batchPreparedStatementSetterClass, String entityListGlobalVariableName) {
		StringBuilder rslt = new StringBuilder()
		.append("public int getBatchSize() {\n");
		CodeBuilderHelper.buildMethodReturn(b -> { b.append(entityListGlobalVariableName).append(".size();"); }, rslt);

		CodeBuilderHelper.buildCloseMethod(rslt);
		System.out.println("#### B: " + rslt.toString());
		return rslt.toString();
	}

	private <T> String generateBatchPreparedStatementSetterSetValues(CtClass batchPreparedStatementSetterClass, String entityListGlobalVariableName, Class<T> entityType, EntityColumnMetaData[] columnMetadatas) {
		String preparedStatementArgumentName = "ps";
		String indexArgumentName = "i";
		String entityVariableName = "entity";
		
		StringBuilder rslt = new StringBuilder()
		.append("public void setValues(java.sql.PreparedStatement ").append(preparedStatementArgumentName).append(", int ").append(indexArgumentName).append(") throws java.sql.SQLException {\n");
		
		CodeBuilderHelper.buildVariableDeclarationAndAssignment(entityType, entityVariableName, 
				b -> {
					CodeBuilderHelper.buildCast(entityType, b).append(entityListGlobalVariableName).append(".get(").append(indexArgumentName).append(");\n");
				}, 
				rslt);

		for (int i = 0; i < columnMetadatas.length; i++) {
			generatePreparedStatementSetter(i + 1, columnMetadatas[i], preparedStatementArgumentName, entityVariableName, DATA_TYPE_MAPPER, rslt);
		}

		CodeBuilderHelper.buildCloseMethod(rslt);
		System.out.println("#### C: " + rslt.toString());
		return rslt.toString();
	}
	
	private void join(EntityColumnMetaData[] columnMetadatas, Function<EntityColumnMetaData, String> elementTranslator, String separator, StringBuilder out) {
		int arrayLength = columnMetadatas.length;		
		for (int i = 0; i < arrayLength -1; i++) {
			out.append(elementTranslator.apply(columnMetadatas[i]));
			out.append(separator);
		}
		if (arrayLength > 0) {
			out.append(elementTranslator.apply(columnMetadatas[arrayLength - 1]));
		}
	}
	
	private void generatePreparedStatementSetter(int index, EntityColumnMetaData columnMetadata, String preparedStatementVariableName, String entityVariableName, Map<Class, DataTypeMetadata> dataTypeMetadata, StringBuilder out) {
		DataTypeMetadata metadata = dataTypeMetadata.get(columnMetadata.getDataType());
		out.append("if (").append(entityVariableName).append(".").append(columnMetadata.getGetterMethodName()).append("() != null) {\n")
			.append(preparedStatementVariableName).append(".set").append(metadata.getPropertyName()).append("(")
				.append(index)
				.append(",")
				.append(entityVariableName).append(".").append(columnMetadata.getGetterMethodName()).append("()");
				if (metadata.getToPrimitiveMethodName() != null) {
					out.append(".").append(metadata.getToPrimitiveMethodName()).append("()");
				}
				out.append(");\n")
		.append("} else {\n");
			CodeBuilderHelper.buildCallVariableMethod(preparedStatementVariableName, "setNull", 
					b -> { b.append(index).append(", ").append(metadata.getSqlType()); }, 
					out)
		.append("}\n");
	}
	
	private String generateGetInsertSql(String tableName, EntityColumnMetaData[] columnMetadatas) {
		StringBuilder rslt = new StringBuilder();
		rslt.append("public String getInsertSql() {\n"); 

		rslt.append("return \"INSERT INTO ").append(tableName).append("(");
		join(columnMetadatas, m -> m.persistentColumnName, ",", rslt);
		rslt.append(") VALUES (");
		join(columnMetadatas, m -> "?", ",", rslt);
		rslt.append(")\";\n");

		CodeBuilderHelper.buildCloseMethod(rslt);
		System.out.println("#### 1 :\n" + rslt.toString());
		return rslt.toString();
	}
	
	private <T> String generateGetBatchPreparedStatementSetter(Class batchPreparedStatementSetterClass, String entitySetterName) {
		String entitiesArgumentName = "entities";
		String resultVariableName = "rslt";
		
		StringBuilder rslt = new StringBuilder();
		rslt.append("public org.springframework.jdbc.core.BatchPreparedStatementSetter getBatchPreparedStatementSetter(").append(List.class.getName()).append(" ").append(entitiesArgumentName).append(") {\n"); 

		CodeBuilderHelper.buildVariableDeclarationAndAssignment(batchPreparedStatementSetterClass, resultVariableName, 
				b -> { CodeBuilderHelper.buildEmptyConstructor(batchPreparedStatementSetterClass, b); }, 
				rslt)
		
		.append(resultVariableName).append(".").append(entitySetterName).append("(").append(entitiesArgumentName).append(");\n");
		CodeBuilderHelper.buildMethodReturn(b -> { b.append(resultVariableName); }, rslt);
		CodeBuilderHelper.buildCloseMethod(rslt);
		System.out.println("#### 2 :\n" + rslt.toString());
		return rslt.toString();
	}
	
	private <T> String generateGetRowMapper(Class rowMapperClass) {
		StringBuilder rslt = new StringBuilder();
		rslt.append("public org.springframework.jdbc.core.RowMapper getRowMapper() {\n");

		CodeBuilderHelper.buildMethodReturn(b -> { CodeBuilderHelper.buildEmptyConstructor(rowMapperClass, b); }, rslt);
		
		CodeBuilderHelper.buildCloseMethod(rslt);
		System.out.println("#### 3 :\n" + rslt.toString());
		return rslt.toString();
		
	}
	
	private <T> String extractEntityTableName(Class<T> entityType) {
		Table tableAnnotation = entityType.getAnnotation(Table.class);
		return (tableAnnotation != null) ? tableAnnotation.name() : null;
	}

	private <T> EntityColumnMetaData[] extractEntityColumns(Class<T> entityType) throws Exception {
		List<EntityColumnMetaData> rslt = new ArrayList<>();
		
		BeanInfo beanInfo = Introspector.getBeanInfo(entityType);
		for (PropertyDescriptor propDesc : beanInfo.getPropertyDescriptors()) {
			try {
				Optional.ofNullable(entityType.getDeclaredField(propDesc.getName()))
					.map(f -> f.getAnnotation(Column.class))
					.map(c -> new EntityColumnMetaData(propDesc, c.name()))
					.map(m -> rslt.add(m));
			} catch (NoSuchFieldException ex) {}
		}
		
		return rslt.toArray(new EntityColumnMetaData[rslt.size()]);
	}
	
	@Getter
	private static class EntityColumnMetaData {
		private final String persistentColumnName;
		private final String getterMethodName;
		private final String setterMethodName;
		private final Class dataType;
		
		public EntityColumnMetaData(PropertyDescriptor propertyDescritor, String persistentColumnName) {
			this.persistentColumnName = persistentColumnName;
			this.getterMethodName = propertyDescritor.getReadMethod().getName();
			this.setterMethodName = propertyDescritor.getWriteMethod().getName();
			this.dataType = propertyDescritor.getPropertyType();
		}
	}
	
	@AllArgsConstructor
	@Getter
	private static class DataTypeMetadata {
		private final Class type;
		private final String propertyName;
		private final int sqlType;
		private final String toPrimitiveMethodName;
		private final Class primitiveType;
	}
}
