package org.aerogear.kafka.cdi.configuration;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@ApplicationScoped
public class ConfigInjectionProducer {

    @SuppressWarnings("unchecked")
    private static <T> Class<T> getClassFromParameterizedType(ParameterizedType type) {
        return (Class) type.getActualTypeArguments()[0];
    }

    private Config config;

    @PostConstruct
    void init() {
        config = ConfigProvider.getConfig();
    }

    @Produces
    @ApplicationScoped
    public Config createConfig() {
        return config;
    }

    @Produces
    @ConfigProperty
    public <T> Optional<T> getOptionalProperty(InjectionPoint injectionPoint) {
        ConfigProperty annotation = injectionPoint.getAnnotated().getAnnotation(ConfigProperty.class);
        Type type = injectionPoint.getType();
        if (type instanceof ParameterizedType) {
            Class<T> typeClass = getClassFromParameterizedType((ParameterizedType) type);

            return ConfigProvider.getConfig().getOptionalValue(annotation.name(), typeClass);
        }

        return Optional.empty();
    }

    @Produces
    @ConfigProperty
    public String getProperty(InjectionPoint injectionPoint) {
        ConfigProperty annotation = injectionPoint.getAnnotated().getAnnotation(ConfigProperty.class);

        final String value = ConfigProvider.getConfig().getValue(annotation.name(), String.class);
        return value;
    }

    @Produces
    @ConfigProperty
    public <T> List<T> getListProperty(InjectionPoint injectionPoint) {
        ConfigProperty annotation = injectionPoint.getAnnotated().getAnnotation(ConfigProperty.class);
        Type type = injectionPoint.getType();
        if (type instanceof ParameterizedType) {
            Class<T> typeClass = getClassFromParameterizedType((ParameterizedType) type);

            Config config = ConfigProvider.getConfig();

            String value = config.getValue(annotation.name(), String.class);

            /*
            if (config instanceof ConfigImpl) {
                return ((ConfigImpl) config).convertList(value, typeClass);
            }
            */

        }

        return null;
    }

    @Produces
    @ConfigProperty
    public <T> Set<T> getSetProperty(InjectionPoint injectionPoint) {
        List<T> values = getListProperty(injectionPoint);

        return new HashSet<>(values);
    }

}
