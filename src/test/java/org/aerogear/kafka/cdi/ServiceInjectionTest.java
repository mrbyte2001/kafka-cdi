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
package org.aerogear.kafka.cdi;

import org.aerogear.kafka.SimpleKafkaProducer;
import org.aerogear.kafka.cdi.beans.KafkaService;
import org.aerogear.kafka.cdi.beans.mock.MockProvider;
import org.aerogear.kafka.cdi.configuration.CustomTestConfigProviderResolver;
import org.aerogear.kafka.cdi.tests.AbstractTestBase;
import org.aerogear.kafka.cdi.tests.KafkaClusterTestBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.config.spi.ConfigProviderResolver;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

@RunWith(Arquillian.class)
public class ServiceInjectionTest extends KafkaClusterTestBase {

    @Deployment
    public static JavaArchive createDeployment() {

        return AbstractTestBase.createFrameworkDeployment()
                .addPackage(KafkaService.class.getPackage())
                .addPackage(MockProvider.class.getPackage())
                .addAsServiceProvider(ConfigProviderResolver.class, CustomTestConfigProviderResolver.class);
    }

    @Inject
    private KafkaService service;

    private ProducerConfig producerConfig(final SimpleKafkaProducer simpleKafkaProducer) throws NoSuchFieldException, IllegalAccessException {
        final Field producerConfig = simpleKafkaProducer.getClass().getSuperclass().getDeclaredField("producerConfig");
        producerConfig.setAccessible(true);
        return (ProducerConfig)producerConfig.get(simpleKafkaProducer);
    }

    @Test
    public void nonNullSimpleProducer() throws Exception {
        final SimpleKafkaProducer simpleKafkaProducer = service.returnSimpleProducer();
        Assertions.assertThat(simpleKafkaProducer).isNotNull();
        final ProducerConfig config = producerConfig(simpleKafkaProducer);
        assertThat(config.getList("bootstrap.servers"), containsInAnyOrder("localhost:9098"));
        assertThat(config.getString("compression.type"), equalTo("snappy"));
    }

    @Test
    public void nonNullExtendedProducer() throws Exception {
        assertThat(service.returnExtendedProducer(), notNullValue());
        final ProducerConfig config = producerConfig(service.returnExtendedProducer());
        assertThat(config.getList("bootstrap.servers"), containsInAnyOrder("localhost:9098"));
        assertThat(config.getString("compression.type"), equalTo("lz4"));
        assertThat(config.getString("acks"), equalTo("0"));
        assertThat(config.getInt("retries"), equalTo(1));
    }

    @Test
    public void nonNullExtended2Producer() throws Exception {
        assertThat(service.returnExtendedProducer2(), notNullValue());
        final ProducerConfig config = producerConfig(service.returnExtendedProducer2());
        assertThat(config.getList("bootstrap.servers"), containsInAnyOrder("localhost:9098"));
        assertThat(config.getString("compression.type"), equalTo("gzip"));
        assertThat(config.getString("acks"), equalTo("0"));
        assertThat(config.getInt("retries"), equalTo(2));
    }
}
