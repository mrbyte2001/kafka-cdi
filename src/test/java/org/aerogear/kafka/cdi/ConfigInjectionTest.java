package org.aerogear.kafka.cdi;

import org.aerogear.kafka.cdi.beans.KafkaService;
import org.aerogear.kafka.cdi.beans.mock.MockProvider;
import org.aerogear.kafka.cdi.configuration.ConfigInjectionProducer;
import org.aerogear.kafka.cdi.configuration.CustomTestConfigProviderResolver;
import org.aerogear.kafka.cdi.tests.AbstractTestBase;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.config.spi.ConfigProviderResolver;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;

@RunWith(Arquillian.class)
public class ConfigInjectionTest {

    @Deployment
    public static JavaArchive createDeployment() {
        return AbstractTestBase.createFrameworkDeployment()
                .addPackage(KafkaService.class.getPackage())
                .addPackage(MockProvider.class.getPackage())
                .addClass(ConfigInjectionProducer.class)
                .addAsServiceProvider(ConfigProviderResolver.class, CustomTestConfigProviderResolver.class);
    }

    @Inject
    @ConfigProperty(name = "bootstrap.servers")
    String servers;

    @Test
    public void testResolvedConfiguration() {
        MatcherAssert.assertThat(servers, CoreMatchers.equalTo("localhost:9098"));
    }
}
