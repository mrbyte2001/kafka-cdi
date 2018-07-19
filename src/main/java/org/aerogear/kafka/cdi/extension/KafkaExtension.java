/**
 * Copyright 2017 Red Hat, Inc, and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.aerogear.kafka.cdi.extension;

import org.aerogear.kafka.ExtendedKafkaProducer;
import org.aerogear.kafka.SimpleKafkaProducer;
import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.cdi.annotation.KafkaStream;
import org.aerogear.kafka.impl.DelegationKafkaConsumer;
import org.aerogear.kafka.impl.DelegationStreamProcessor;
import org.aerogear.kafka.impl.InjectedKafkaProducer;
import org.aerogear.kafka.serialization.CafdiSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Collections.newSetFromMap;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class KafkaExtension<X> implements Extension {

    private final Set<AnnotatedMethod<?>> listenerMethods = newSetFromMap(new ConcurrentHashMap<>());
    private final Set<AnnotatedMethod<?>> streamProcessorMethods = newSetFromMap(new ConcurrentHashMap<>());
    private final Set<DelegationKafkaConsumer> managedConsumers = newSetFromMap(new ConcurrentHashMap<>());
    private final Set<org.apache.kafka.clients.producer.Producer> managedProducers = newSetFromMap(new ConcurrentHashMap<>());
    private final Logger logger = LoggerFactory.getLogger(KafkaExtension.class);
    private List<KafkaBean> beans = new ArrayList<>();

    public void createConfigBuilder(@Observes BeforeBeanDiscovery beforeBeanDiscovery){

    }

    public void collectConfiguration(@Observes @WithAnnotations({KafkaConfig.class}) ProcessAnnotatedType<X> pat) {
        final List<Field> fields = asList(pat.getAnnotatedType().getJavaClass().getDeclaredFields());

    }

    public void registerListeners(@Observes @WithAnnotations({Consumer.class, KafkaStream.class}) ProcessAnnotatedType<X> pat) {

        logger.trace("scanning type: " + pat.getAnnotatedType().getJavaClass().getName());
        final AnnotatedType<X> annotatedType = pat.getAnnotatedType();


        for (AnnotatedMethod am : annotatedType.getMethods()) {

            if (am.isAnnotationPresent(Consumer.class)) {

                logger.debug("found annotated listener method, adding for further processing");

                listenerMethods.add(am);
            } else if (am.isAnnotationPresent(KafkaStream.class)) {

                logger.debug("found annotated stream method, adding for further processing");

                streamProcessorMethods.add(am);
            }

        }
    }

    public void simple(@Observes ProcessInjectionPoint<X,SimpleKafkaProducer> pat) {
        final InjectionPoint injectionPoint = pat.getInjectionPoint();
        Config config = ConfigProvider.getConfig();
        final KafkaConfig kafkaConfig = injectionPoint.getAnnotated().getAnnotation(KafkaConfig.class);

        final Serde<?> keySerde = CafdiSerdes.serdeFrom((Class<?>)  ((ParameterizedType)injectionPoint.getType()).getActualTypeArguments()[0]);
        final Serde<?> valSerde = CafdiSerdes.serdeFrom((Class<?>)  ((ParameterizedType)injectionPoint.getType()).getActualTypeArguments()[1]);

        Properties properties = configurationProperties(config, kafkaConfig);

        final String namespace = kafkaConfig != null ? kafkaConfig.value() : "";

        final InjectedKafkaProducer injectionProducer = createInjectionProducer(namespace, properties, config, keySerde.serializer().getClass(),
                valSerde.serializer().getClass(),
                keySerde.serializer(),
                valSerde.serializer());

        final Set<Type> beanTypes = new HashSet<>(Arrays.asList(injectionPoint.getType(), Object.class));

        final KafkaBean<SimpleKafkaProducer> kafkaBean = new KafkaBean<>(
                SimpleKafkaProducer.class,
                beanTypes,
                pat.getInjectionPoint().getQualifiers(), injectionProducer);

        beans.add(kafkaBean);
    }

    private Properties configurationProperties(final Config config, final KafkaConfig kafkaConfig) {
        final String configurationName = kafkaConfig != null ? kafkaConfig.value() : "";

        Properties properties = new Properties();

        StreamSupport.stream(config.getConfigSources().spliterator(), false)
                .filter(configSource -> configSource.getName().equals("DEFAULT"))
                .findFirst()
                .map(ConfigSource::getProperties)
                .ifPresent(properties::putAll);

        if (isNotEmpty(configurationName)) {
            StreamSupport.stream(config.getConfigSources().spliterator(), false)
                    .filter(configSource -> configSource.getName().equals(configurationName))
                    .findFirst()
                    .map(ConfigSource::getProperties)
                    .ifPresent(properties::putAll);
        }

        return properties;
    }

    public void extended(@Observes ProcessInjectionPoint<X,ExtendedKafkaProducer> pat) {
        final InjectionPoint injectionPoint = pat.getInjectionPoint();
        Config config = ConfigProvider.getConfig();
        final KafkaConfig kafkaConfig = injectionPoint.getAnnotated().getAnnotation(KafkaConfig.class);

        final Serde<?> keySerde = CafdiSerdes.serdeFrom((Class<?>)  ((ParameterizedType)injectionPoint.getType()).getActualTypeArguments()[0]);
        final Serde<?> valSerde = CafdiSerdes.serdeFrom((Class<?>)  ((ParameterizedType)injectionPoint.getType()).getActualTypeArguments()[1]);

        Properties properties = configurationProperties(config, kafkaConfig);

        final String namespace = kafkaConfig != null ? kafkaConfig.value() : "";

        final InjectedKafkaProducer injectionProducer = createInjectionProducer(namespace, properties, config, keySerde.serializer().getClass(),
                valSerde.serializer().getClass(),
                keySerde.serializer(),
                valSerde.serializer());

        final Set<Type> beanTypes = new HashSet<>(Arrays.asList(injectionPoint.getType(), Object.class));

        final KafkaBean<ExtendedKafkaProducer> kafkaBean = new KafkaBean<>(
                ExtendedKafkaProducer.class,
                beanTypes,
                pat.getInjectionPoint().getQualifiers(), injectionProducer);

        beans.add(kafkaBean);
    }

    public void afterDeploymentValidation(@Observes AfterDeploymentValidation adv, final BeanManager bm) {

//        final BeanManager bm = CDI.current().getBeanManager();

        logger.trace("wiring annotated methods to internal Kafka Util clazzes");

        listenerMethods.forEach( consumerMethod -> {

            final Bean<DelegationKafkaConsumer> bean = (Bean<DelegationKafkaConsumer>) bm.getBeans(DelegationKafkaConsumer.class).iterator().next();
            final CreationalContext<DelegationKafkaConsumer> ctx = bm.createCreationalContext(bean);
            final DelegationKafkaConsumer frameworkConsumer = (DelegationKafkaConsumer) bm.getReference(bean, DelegationKafkaConsumer.class, ctx);

            // hooking it all together
            frameworkConsumer.initialize(consumerMethod, bm);

            managedConsumers.add(frameworkConsumer);
            submitToExecutor(frameworkConsumer);
        });


        streamProcessorMethods.forEach(annotatedStreamMethod -> {
            final Bean<DelegationStreamProcessor> bean = (Bean<DelegationStreamProcessor>) bm.getBeans(DelegationStreamProcessor.class).iterator().next();
            final CreationalContext<DelegationStreamProcessor> ctx = bm.createCreationalContext(bean);
            final DelegationStreamProcessor frameworkProcessor = (DelegationStreamProcessor) bm.getReference(bean, DelegationStreamProcessor.class, ctx);

            // TODO: Add Configuration
            frameworkProcessor.init(annotatedStreamMethod, bm);
        });

    }

    public void beforeShutdown(@Observes final BeforeShutdown bs) {
        managedConsumers.forEach(DelegationKafkaConsumer::shutdown);

        managedProducers.forEach(org.apache.kafka.clients.producer.Producer::close);
    }

    public void registerBeans(@Observes AfterBeanDiscovery abd) {
        logger.trace("Register beans.");
        beans.forEach(abd::addBean);
    }

    private boolean isNotEmpty(final String value) {
        return value != null && value.trim().length() > 0;
    }

    private void submitToExecutor(final DelegationKafkaConsumer delegationKafkaConsumer) {

        ExecutorService executorService;
        try {
            executorService = InitialContext.doLookup("java:comp/DefaultManagedExecutorService");
        } catch (NamingException e) {
            logger.warn("Could not find a managed ExecutorService, creating one manually");
            executorService = new ThreadPoolExecutor(16, 16, 10, TimeUnit.MINUTES, new LinkedBlockingDeque<Runnable>());
        }

        // submit the consumer
        executorService.execute(delegationKafkaConsumer);
    }

    private InjectedKafkaProducer createInjectionProducer(final String namespace, final Properties properties, final Config config, final Class<?> keySerializerClass, final Class<?> valSerializerClass, final Serializer<?> keySerializer, final Serializer<?> valSerializer) {


        final Map<String, Object> configurationProperties = replaceValues(namespace, properties, config);

        configurationProperties.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        configurationProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, valSerializerClass);

        return new InjectedKafkaProducer(configurationProperties, keySerializer, valSerializer);
    }

    private Map<String, Object> replaceValues(final String namespace, final Properties properties, final Config config) {
        return properties
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            e -> String.valueOf(e.getKey()),
                            e -> config.getOptionalValue(resolveConfigKey(namespace, e.getKey().toString()), String.class).orElse(String.valueOf(e.getValue()))
                    ));
    }

    private String resolveConfigKey(final String prefix, final String key) {
        String resolvedKey = key;

        if(isNotEmpty(prefix)) {
            resolvedKey = String.format("%s#%s", prefix, key);
        }

        return resolvedKey;
    }


}
