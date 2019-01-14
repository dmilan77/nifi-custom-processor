package org.apache.nifi.processors.hazelcast.zookeeper.service;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.zookeeper.ZookeeperDiscoveryProperties;
import com.hazelcast.zookeeper.ZookeeperDiscoveryStrategyFactory;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class HazlecastZookeeperClientService extends AbstractControllerService implements HazlecastService {
    public static final PropertyDescriptor ZOOKEEPER_QUORUM = new PropertyDescriptor
            .Builder().name("ZOOKEEPER_QUORUM")
            .displayName("ZOOKEEPER_QUORUM")
            .description("ZOOKEEPER_QUORUM")
            .defaultValue("localhost:2181")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ZOOKEEPER_PATH = new PropertyDescriptor
            .Builder().name("ZOOKEEPER_PATH")
            .displayName("ZOOKEEPER_PATH")
            .description("ZOOKEEPER_PATH")
            .defaultValue("/hazlecast1")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CLUSTER_NAME = new PropertyDescriptor
            .Builder().name("CLUSTER_NAME")
            .displayName("CLUSTER_NAME")
            .description("CLUSTER_NAME")
            .defaultValue("hazlecast1")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;
    private ReentrantLock lock;
    private AtomicReference<HazelcastInstance> hazleCastClient = new AtomicReference<HazelcastInstance>();
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ZOOKEEPER_QUORUM);
        props.add(ZOOKEEPER_PATH);
        props.add(CLUSTER_NAME);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ControllerServiceInitializationContext config) throws InitializationException {
        lock = new ReentrantLock();
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        final String zookeeperURL = context.getProperty(ZOOKEEPER_QUORUM).getValue();
        final String zookeeperPATH = context.getProperty(ZOOKEEPER_PATH).getValue();
        final String clusterNAME = context.getProperty(CLUSTER_NAME).getValue();
        getLogger().info("Hazlecast Client Staring .. "+ clusterNAME);
        lock.lock();
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().getAwsConfig().setEnabled(false);
        config.setProperty(GroupProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(new ZookeeperDiscoveryStrategyFactory());
        discoveryStrategyConfig.addProperty(ZookeeperDiscoveryProperties.ZOOKEEPER_URL.key(), zookeeperURL);
        discoveryStrategyConfig.addProperty(ZookeeperDiscoveryProperties.ZOOKEEPER_PATH.key(), zookeeperPATH);
        discoveryStrategyConfig.addProperty(ZookeeperDiscoveryProperties.GROUP.key(), clusterNAME);
        config.getNetworkConfig().getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);

        hazleCastClient.set(HazelcastClient.newHazelcastClient(config));
        lock.unlock();

    }

    @OnDisabled
    public void shutdown() {

        getLogger().info("Hazlecast Shutting down.. "+ this.hazleCastClient.get());
//        lock.lock();
        if(this.hazleCastClient.get() != null) this.hazleCastClient.get().shutdown();
//        lock.unlock();

    }

    @Override
    public void execute() throws ProcessException {

    }
    @Override
    public void put(final String mapName, final Object key, final Object value)  throws ProcessException{
        IMap hazleMap =  hazleCastClient.get().getMap(mapName);
        hazleMap.put(key,value);
    }
    @Override
    public Object get(final String mapName, final Object key)  throws ProcessException {
        IMap hazleMap =  hazleCastClient.get().getMap(mapName);
        return hazleMap.get(key);
    }
}
