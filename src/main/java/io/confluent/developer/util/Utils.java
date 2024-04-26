package io.confluent.developer.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 4/26/24
 * Time: 10:25 AM
 */
public class Utils {


    public static Properties getProperties(String propertiesFile) {
         Properties props = new Properties();
         try (InputStream input = Utils.class.getClassLoader().getResourceAsStream(propertiesFile)) {
             props.load(input);
             return props;
         } catch (IOException e) {
             throw new RuntimeException(e);
         }
    }

    public static Properties getProperties() {
        return getProperties("config.properties");
    }
}
