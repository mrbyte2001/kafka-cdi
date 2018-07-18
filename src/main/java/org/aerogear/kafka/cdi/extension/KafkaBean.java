package org.aerogear.kafka.cdi.extension;


import javax.enterprise.context.Dependent;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.InjectionPoint;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Set;

public class KafkaBean<T> implements Bean<T> {

    private Class<?> beanClass;

    private Set<Type> beanTypes;

    private Set<Annotation> qualifiers;

    private final T injectionProducer;

    private Set<Class<? extends Annotation>> stereotypes;
    private final Set<InjectionPoint> injectionPoints;

    /**
     * Constructor.
     *  @param beanClass
     *            the bean class
     * @param beanTypes
     *            the bean types
     * @param qualifiers
     */
    public KafkaBean(final Class<?> beanClass, final Set<Type> beanTypes,
                     final Set<Annotation> qualifiers, T injectionProducer) {
        this.beanClass = beanClass;
        this.beanTypes = Collections.unmodifiableSet(beanTypes);
        this.qualifiers = Collections.unmodifiableSet(qualifiers);
        this.injectionProducer = injectionProducer;
        this.stereotypes = Collections.emptySet();
        this.injectionPoints = Collections.emptySet();
    }
    @Override
    public Set<Type> getTypes() {
        return this.beanTypes;
    }

    @Override
    public Set<Annotation> getQualifiers() {
        return this.qualifiers;
    }

    @Override
    public Class<? extends Annotation> getScope() {
        return Dependent.class;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public Set<Class<? extends Annotation>> getStereotypes() {
        return this.stereotypes;
    }

    @Override
    public boolean isAlternative() {
        return false;
    }


    @Override
    public Class<?> getBeanClass() {
        return this.beanClass;
    }

    @Override
    public Set<InjectionPoint> getInjectionPoints() {
        return this.injectionPoints;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public T create(final CreationalContext<T> creationalContext) {
        return injectionProducer;
    }

    @Override
    public void destroy(final T t, final CreationalContext<T> creationalContext) {
        creationalContext.release();
    }
}