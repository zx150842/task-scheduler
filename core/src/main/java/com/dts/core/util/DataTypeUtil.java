package com.dts.core.util;

/**
 * @author zhangxin
 */
public class DataTypeUtil {

  public static Object convertToPrimitiveType(String type, String value) {
    Object convertValue = null;
    switch (type) {
      case "boolean":
      case "Boolean":
        convertValue = Boolean.valueOf(value);
        break;
      case "byte":
      case "Byte":
        convertValue = Byte.valueOf(value);
        break;
      case "short":
      case "Short":
        convertValue = Short.valueOf(value);
        break;
      case "int":
      case "Integer":
        convertValue = Integer.valueOf(value);
        break;
      case "long":
      case "Long":
        convertValue = Long.valueOf(value);
        break;
      case "float":
      case "Float":
        convertValue = Float.valueOf(value);
        break;
      case "double":
      case "Double":
        convertValue = Double.valueOf(value);
        break;
      case "String":
        convertValue = value;
        break;
      default:
        throw new IllegalArgumentException("Cannot parse task method params data type " + type);
    }
    return convertValue;
  }
}
