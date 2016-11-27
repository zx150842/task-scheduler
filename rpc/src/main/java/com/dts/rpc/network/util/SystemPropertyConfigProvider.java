package com.dts.rpc.network.util;

import java.util.NoSuchElementException;

/**
 * Created by zhangxin on 2016/11/27.
 */
public class SystemPropertyConfigProvider {

    public static String get(String name) {
        String value = System.getProperty(name);
        if (value == null) {
            throw new NoSuchElementException(name);
        }
        return value;
    }

    public static String get(String name, String defaultValue) {
        try {
            return get(name);
        } catch (NoSuchElementException e) {
            return defaultValue;
        }
    }

    public static int getInt(String name, int defaultValue) {
        return Integer.parseInt(get(name, Integer.toString(defaultValue)));
    }

    public static long getLong(String name, int defaultValue) {
        return Long.parseLong(get(name, Long.toString(defaultValue)));
    }

    public static double getDouble(String name, double defaultValue) {
        return Double.parseDouble(get(name, Double.toString(defaultValue)));
    }

    public static boolean getBoolean(String name, boolean defaultValue) {
        return Boolean.parseBoolean(get(name, Boolean.toString(defaultValue)));
    }
}
