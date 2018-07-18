package org.aerogear.kafka.cdi.extension;

import org.eclipse.microprofile.config.spi.ConfigSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaPropertiesConfigSource implements ConfigSource {

    static final String TEMPLATE_CONFIG_NAME = "kafka-*.properties";

    private static final String DEFAULT_CONFIG_NAME = "META-INF/kafka.properties";

    private static final String PREFIX = "%s#";

    private final Map<String, String> properties;

    private final String name;

    private final int ordinal;

    private KafkaPropertiesConfigSource(final Properties properties, final String name, int ordinal) {

        this.name = Objects.requireNonNull(name);
        this.ordinal = ordinal;
        this.properties = toMap(properties);
    }

    private Map<String, String> toMap(final Properties properties) {
        return Objects.requireNonNull(properties).entrySet().stream().collect(
               Collectors.toMap(
                       e -> e.getKey().toString(),
                       e -> e.getValue().toString())
        );
    }

    static ConfigSource defaultConfig(final ClassLoader forClassLoader) {
        final Properties properties = loadProperties(forClassLoader, DEFAULT_CONFIG_NAME);
        return new KafkaPropertiesConfigSource(properties, "DEFAULT", 100);
    }

    static ConfigSource templateConfig(final ClassLoader forClassLoader, final String name) {
        final Properties properties = loadProperties(forClassLoader, "META-INF/" +name);

        return new KafkaPropertiesConfigSource(properties, extractNameWithoutPrefixAndExtension(name), 150);
    }

    private static Properties loadProperties(final ClassLoader forClassLoader, final String configurationName)  {
        final InputStream property = forClassLoader.getResourceAsStream(configurationName);

        Properties properties = new Properties();
        try {
            if (property != null) {
                properties.load(property);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return properties;
    }

    private static String extractNameWithoutPrefixAndExtension(final String name) {
        final int pointBeforeFileExtension = name.lastIndexOf('.');
        final int indexOfFirstHyphen = name.indexOf('-');

        return pointBeforeFileExtension != -1 && pointBeforeFileExtension < name.length() ? name.substring(indexOfFirstHyphen+1, pointBeforeFileExtension) : name;
    }

    @Override
    public int getOrdinal() {
        final int ordinal = ConfigSource.super.getOrdinal();
        return ordinal > 100 ? ordinal : this.ordinal;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String getValue(final String propertyName) {

        if (isNamespacedProperty(propertyName)) {
            return nameMatchesNamespace(propertyName) ? properties.get(removeNamespace(propertyName)) : null;
        } else if (isDefaultConfiguration()){
            return properties.get(propertyName);
        }

        return null;
    }

    private boolean isDefaultConfiguration() {
        return getName().equals("DEFAULT");
    }

    private String removeNamespace(String key) {
        return Objects.requireNonNull(key).replaceFirst(prefix(), "");
    }

    private boolean nameMatchesNamespace(final String propertyName) {
        return propertyName.startsWith(prefix());
    }

    private boolean isNamespacedProperty(final String propertyName) {
        return propertyName.contains("#");
    }

    private String prefix() {
        return String.format(PREFIX, getName());
    }

    @Override
    public String getName() {
        return name;
    }

}
