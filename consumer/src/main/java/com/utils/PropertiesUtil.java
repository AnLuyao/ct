package com.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author AnLuyao
 * @date 2018-06-01 15:30
 */
public class PropertiesUtil {
    public static Properties properties;
    static{
        try {
            // 加载配置属性
            InputStream inputStream = ClassLoader.getSystemResourceAsStream("kafka_hbase.properties");
            properties = new Properties();
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key){
        return properties.getProperty(key);
    }
}
