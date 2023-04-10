package com.ddv.test.codegen;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Embedded;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

public class StringCutterFactory {

	private Map<Class<?>, Class<? extends StringCutter>> typeToStringCutterTypeMap = new HashMap<>();
	
	public Class<? extends StringCutter> generateStringCutter(Class<?> sourceType) throws Exception {
		Class<? extends StringCutter> rslt = typeToStringCutterTypeMap.get(sourceType);
		
		if (rslt == null) {
			int count = 0;
			CtClass stringCutterClass = createCtClass(sourceType);
			
			String stringCutterMethodName = getStringCutterMethodName();
			String stringCutterMethodArgName = "obj";
			String stringCutterMethodCastArgName = "objc";
			
			StringBuilder methodBodyCode = new StringBuilder();
			methodBodyCode.append("public void ").append(stringCutterMethodName).append("(Object ").append(stringCutterMethodArgName).append(") {\n")
				.append(sourceType.getName()).append(" ").append(stringCutterMethodCastArgName).append(" = (").append(sourceType.getName()).append(")").append(stringCutterMethodArgName).append(";\n");
			
			BeanInfo info = Introspector.getBeanInfo(sourceType);
			for (PropertyDescriptor propDesc : info.getPropertyDescriptors()) {
				if ((propDesc.getReadMethod() != null) && (propDesc.getWriteMethod() != null)) {
					String fieldName = Introspector.decapitalize(propDesc.getWriteMethod().getName().substring(3));
					Field field = getField(sourceType, fieldName);
					
					if (!processStringField(field, propDesc, methodBodyCode, stringCutterMethodCastArgName)) {
						processEmbeddedField(field, propDesc, methodBodyCode, stringCutterMethodName, stringCutterMethodCastArgName, count++);
					}
				}
			}
			
			methodBodyCode.append("}");
			
			stringCutterClass.addMethod(CtMethod.make(methodBodyCode.toString(), stringCutterClass));
			
			rslt = (Class<? extends StringCutter>) stringCutterClass.toClass();
			typeToStringCutterTypeMap.put(sourceType, rslt);
		}
		
		return rslt;
	}

	private boolean processStringField(Field field, PropertyDescriptor propDesc, StringBuilder stringCutterMethodCode, String stringCutterMethodSourceObjParamName) throws Exception {
		Column columnAnn = findColumnAnnotationOnStringField(field, propDesc.getReadMethod());
		if (columnAnn != null) {
			stringCutterMethodCode
				.append("String " + field.getName() + " = ").append(stringCutterMethodSourceObjParamName).append(".").append(propDesc.getReadMethod().getName()).append("();\n")
				.append("if (").append(field.getName()).append(" != null) {\n")
				.append("   if (").append(field.getName()).append(".length() > ").append(columnAnn.length()).append(") {\n")
				.append("      ").append(stringCutterMethodSourceObjParamName).append(".").append(propDesc.getWriteMethod().getName()).append("(").append(field.getName()).append(".substring(0, ").append(columnAnn.length()).append("));\n")
				.append("}}\n");
			return true;
		}
		return false;
	}
	
	private boolean processEmbeddedField(Field field, PropertyDescriptor propDesc, StringBuilder stringCutterMethodCode, String stringCutterMethodName, String stringCutterMethodSourceObjParamName, int count) throws Exception {
		Embedded embeddedAn = findEmbeddedAnnotation(field, propDesc.getReadMethod());
		if (embeddedAn != null) {
			Class<?> fieldType = field.getType();
			String varName = field.getName() + count;
			
			Class<? extends StringCutter> fieldTypeStringCutterClass = generateStringCutter(fieldType);
			
			stringCutterMethodCode
				.append("Object ").append(varName).append(" = ").append(stringCutterMethodSourceObjParamName).append(".").append(propDesc.getReadMethod().getName()).append("();\n")
				.append("if (").append(varName).append(" != null) {\n")
				.append("   ").append(StringCutter.class.getName()).append(" t = ").append(fieldTypeStringCutterClass.getName()).append(".class.newInstance();\n")
				.append("   t.").append(stringCutterMethodName).append("(").append(varName).append(");\n")
				.append("}\n");
			return true;
		}
		return false;
	}
	
	private Column findColumnAnnotationOnStringField(Field field, Method getter) throws Exception {
		if (String.class.isAssignableFrom(field.getType())) {
			return findAnnotation(Column.class, field, getter);
		}
		return null;
	}

	private Embedded findEmbeddedAnnotation(Field field, Method getter) throws Exception {
		return findAnnotation(Embedded.class, field, getter);
	}
	
	private <T extends Annotation> T findAnnotation(Class<T> annotationType, Field field, Method getter) throws Exception {
		T annotation = field.getAnnotation(annotationType);
		if (annotation == null) {
			annotation = getter.getAnnotation(annotationType);
		}
		return annotation;
	}
	
	private CtClass createCtClass(Class<?> sourceType) throws Exception {
		ClassPool classPool = ClassPool.getDefault();
		CtClass ctClass = classPool.makeClass(sourceType.getName() + "StringCutter");
		ctClass.addInterface(classPool.get(StringCutter.class.getName()));
		return ctClass;
	}
	
	private String getStringCutterMethodName() {
		for (Method method : StringCutter.class.getDeclaredMethods()) {
			if ((Void.TYPE.equals(method.getReturnType())) && (method.getParameterCount() == 1) && (Object.class.equals(method.getParameters()[0].getType()))) {
				return method.getName();
			}
		}
		return null;
	}
	
	private Field getField(Class<?> clazz, String fieldName) throws Exception {
		try {
			return clazz.getDeclaredField(fieldName);
		} catch (NoSuchFieldException ex) {
			if (clazz.getSuperclass() != null) {
				return getField(clazz.getSuperclass(), fieldName);
			}
			return null;
		}
	}
}
