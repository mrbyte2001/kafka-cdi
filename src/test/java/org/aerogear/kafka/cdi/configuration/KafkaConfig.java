package org.aerogear.kafka.cdi.configuration;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KafkaConfig implements Config {

    private final Iterable<ConfigSource> configSources;
    private final Set<String> propertyNames;

    public KafkaConfig(final List<ConfigSource> sources) {

        this.configSources = sources;
        propertyNames = StreamSupport.stream(configSources.spliterator(), false)
                .map(ConfigSource::getPropertyNames)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public <T> T getValue(final String propertyName, final Class<T> propertyType) {
        final Stream<ConfigSource> stream = StreamSupport.stream(configSources.spliterator(), false);
        return stream.sorted((f1, f2) -> Integer.compare(f2.getOrdinal(), f1.getOrdinal()))
                .filter(configSource -> configSource.getValue(propertyName) != null)
                .findFirst()
                .map(configSource -> (T) configSource.getValue(propertyName)).orElse(null);
    }

    @Override
    public <T> Optional<T> getOptionalValue(final String propertyName, final Class<T> propertyType) {
        final Stream<ConfigSource> stream = StreamSupport.stream(configSources.spliterator(), false);
        return stream.sorted((f1, f2) -> Integer.compare(f2.getOrdinal(), f1.getOrdinal()))
                .filter(configSource -> configSource.getValue(propertyName) != null)
                .findFirst()
                .map(configSource -> (T) configSource.getValue(propertyName));
    }

    @Override
    public Iterable<String> getPropertyNames() {

        return propertyNames;
    }

    @Override
    public Iterable<ConfigSource> getConfigSources() {
        return configSources;
    }

    <T> T convert(final String s, final T configurationPropertyType) {
        return (T) configurationPropertyType.getClass().cast(s);
    }
}
