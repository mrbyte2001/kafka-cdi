package org.aerogear.kafka.cdi.configuration;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigBuilder;
import org.eclipse.microprofile.config.spi.ConfigProviderResolver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CustomTestConfigProviderResolver extends ConfigProviderResolver {

    private Map<ClassLoader, Config> configs = new ConcurrentHashMap<>();

    @Override
    public Config getConfig() {
        return getConfig(Thread.currentThread().getContextClassLoader());
    }

    @Override
    public Config getConfig(final ClassLoader loader) {
        Config config = this.configs.get(loader);
        if (config == null) {

            config = new DefaultConfigBuilder()
                    .addDiscoveredFilters()
                    .addDiscoveredConverters()
                    .addDefaultSources()
                    .addDiscoveredSources()
                    .build();
            registerConfig(config, loader);
        }
        return config;
    }

    @Override
    public ConfigBuilder getBuilder() {
        return new DefaultConfigBuilder();
    }

    @Override
    public void registerConfig(Config config, ClassLoader classLoader) {
        if (classLoader == null) {
            classLoader = Thread.currentThread().getContextClassLoader();
        }
        if (configs.containsKey(classLoader)) {
            // LOG.warning("Replacing existing config for classloader: " + classLoader);
        }
        //LOG.info( "Registering config for classloader: " + classLoader + ": " + config);
        this.configs.put(classLoader, config);
    }

    @Override
    public void releaseConfig(Config config) {
        for (Map.Entry<ClassLoader, Config> en : this.configs.entrySet()) {
            if (en.getValue().equals(config)) {
                //LOG.info( "Releasing config for classloader: " + en.getKey());
                this.configs.remove(en.getKey());
                return;
            }
        }
    }
}
