package org.aerogear.kafka.cdi.extension;

import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.ConfigSourceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

public class KafkaConfigSourceProvider implements ConfigSourceProvider {

    private final Logger logger = LoggerFactory.getLogger(KafkaConfigSourceProvider.class);

    private final List<ConfigSource> configSources = new ArrayList<>();

    @Override
    public Iterable<ConfigSource> getConfigSources(final ClassLoader forClassLoader) {

        final ConfigSource defaultConfigSource = KafkaPropertiesConfigSource.defaultConfig(forClassLoader);
        if(!defaultConfigSource.getPropertyNames().isEmpty()) {
            logger.info("Add kafka.properties to Kafka Configuration.");
            configSources.add(defaultConfigSource);
        }

        try {
            final Enumeration<URL> metaInfDirs = forClassLoader.getResources("META-INF");
            while(metaInfDirs.hasMoreElements()) {
                final URI uri = metaInfDirs.nextElement().toURI();

                if("jar".equals(uri.getScheme())){
                    for (FileSystemProvider provider: FileSystemProvider.installedProviders()) {
                        if (provider.getScheme().equalsIgnoreCase("jar")) {
                            try {
                                provider.getFileSystem(uri);
                            } catch (FileSystemNotFoundException e) {
                                // in this case we need to initialize it first:
                                provider.newFileSystem(uri, Collections.emptyMap());
                            }
                        }
                    }
                }

                try(DirectoryStream<Path> dirStream = Files.newDirectoryStream(Paths.get(uri), KafkaPropertiesConfigSource.TEMPLATE_CONFIG_NAME)) {
                    dirStream.forEach(path -> {
                        final ConfigSource configSource = loadConfiguration(forClassLoader, path);
                        logger.info("Add {}.properties to Kafka Configuration.", configSource.getName());
                        configSources.add(configSource);
                    });
                }
            }

        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }

        return Collections.unmodifiableList(configSources);
    }

    private ConfigSource loadConfiguration(ClassLoader forClassLoader, final Path pathToConfiguration) {

        return KafkaPropertiesConfigSource.templateConfig(forClassLoader, pathToConfiguration.getFileName().toString());
    }
}
