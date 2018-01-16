package uk.tommyt.kafka.TopicClean;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

public class Config {
    Properties properties;

    Config(String filename) throws IOException {
        this.properties = new Properties();
        try (InputStream input = getClass().getResourceAsStream(filename)){
            if (input == null) {
                throw new IOException(String.format("Could not open %s", filename));
            }
            properties.load(input);
        }
    }

    Config(Properties properties) {
        this.properties = new Properties();
        this.properties.putAll(properties);
    }

    Properties propertiesFor(String prefix) {
        Properties newProperties = new Properties();
        for (Enumeration<?> e = properties.propertyNames(); e.hasMoreElements();) {
            String key = e.nextElement().toString();
            if (key.startsWith(prefix + ".")) {
                newProperties.setProperty(key.substring(prefix.length() + 1), properties.get(key).toString());
            }
        }
        return newProperties;
    }

    Object getProperty(String key) {
        return properties.getProperty(key);
    }
}
