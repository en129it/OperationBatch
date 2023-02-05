package com.ddv.test.codegen;

import java.util.function.Consumer;

public abstract class CodeBuilderHelper {

	public static final String SEMI_COLUMN = ";";
	public static final String NEW_LINE = "\n";
	public static final String END_INSTRUCTION = SEMI_COLUMN + NEW_LINE;
	
	public static StringBuilder buildMethodReturn(Consumer<StringBuilder> variableProvider, StringBuilder buffer) {
		buffer.append("return ");
		variableProvider.accept(buffer);
		buffer.append(END_INSTRUCTION);
		return buffer;
	}
	
	public static StringBuilder buildEmptyConstructor(Class clazz, StringBuilder buffer) {
		buffer.append(" new ").append(clazz.getName()).append("()").append(END_INSTRUCTION);
		return buffer;
	}
	
	public static StringBuilder buildVariableDeclaration(Class variableType, String variableName, StringBuilder buffer) {
		buffer.append(variableType.getName()).append(" ").append(variableName);
		return buffer;
	}
	
	public static StringBuilder buildCloseMethod(StringBuilder buffer) {
		buffer.append("}").append(NEW_LINE);
		return buffer;
	}
	
	public static StringBuilder buildVariableDeclarationAndAssignment(Class variableType, String variableName, Consumer<StringBuilder> assignmentProvider, StringBuilder buffer) {
		buildVariableDeclaration(variableType, variableName, buffer);
		buffer.append(" = ");
		assignmentProvider.accept(buffer);
		return buffer;
	}
	
	public static StringBuilder buildCast(Class castType, StringBuilder buffer) {
		buffer.append("(").append(castType.getName()).append(")");
		return buffer;
	}
	
	public static StringBuilder buildThisFieldAssignment(String fieldName, Consumer<StringBuilder> assignmentProvider, StringBuilder buffer) {
		buffer.append("this.").append(fieldName).append(" = ");
		assignmentProvider.accept(buffer);
		return buffer;
	}
	
	public static StringBuilder buildCallVariableMethod(String variableName, String methodName, Consumer<StringBuilder> methodArgumentsProvider, StringBuilder buffer) {
		buffer.append(variableName).append(".").append(methodName).append("(");
		
		if (methodArgumentsProvider != null) {
			methodArgumentsProvider.accept(buffer);	
		}
		
		buffer.append(")").append(END_INSTRUCTION);
		return buffer;
	}
}
