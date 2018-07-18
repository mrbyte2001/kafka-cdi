package org.aerogear.kafka.cdi.configuration;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigBuilder;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.ConfigSourceProvider;
import org.eclipse.microprofile.config.spi.Converter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;

public class DefaultConfigBuilder implements ConfigBuilder {

    private final List<ConfigSource> sources = new ArrayList<>();

    @Override
    public ConfigBuilder addDefaultSources() {
        return this;
    }

    @Override
    public ConfigBuilder addDiscoveredSources() {
        final ServiceLoader<ConfigSource> configSources = ServiceLoader.load(ConfigSource.class);
        for (ConfigSource configSource : configSources) {
            sources.add(configSource);
        }

        ServiceLoader<ConfigSourceProvider> configSourceProviders = ServiceLoader.load(ConfigSourceProvider.class);
        for (ConfigSourceProvider configSourceProvider : configSourceProviders) {
            final Iterable<ConfigSource> foundConfigSources = configSourceProvider.getConfigSources(Thread.currentThread().getContextClassLoader());

            if(foundConfigSources != null) {
                for (ConfigSource foundConfigSource : foundConfigSources) {
                    sources.add(foundConfigSource);
                }
            }
        }
        return this;
    }

    @Override
    public ConfigBuilder addDiscoveredConverters() {
        return this;
    }

    @Override
    public ConfigBuilder forClassLoader(final ClassLoader loader) {
        return this;
    }

    @Override
    public ConfigBuilder withSources(final ConfigSource... sources) {
        this.sources.addAll(Arrays.asList(sources));
        return this;
    }

    @Override
    public ConfigBuilder withConverters(final Converter<?>... converters) {
        return this;
    }

    @Override
    public Config build() {
        return new KafkaConfig(this.sources);
    }

    public ConfigBuilder addDiscoveredFilters() {
        return this;
    }
}
