package uk.tommyt.kafka.TopicClean;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

public class Config {
    Properties properties = new Properties();

    Config(String filename) throws IOException {
        try (InputStream input = getClass().getResourceAsStream(filename)){
            if (input == null) {
                throw new IOException(String.format("Could not open %s", filename));
            }
            properties.load(input);
        }
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
