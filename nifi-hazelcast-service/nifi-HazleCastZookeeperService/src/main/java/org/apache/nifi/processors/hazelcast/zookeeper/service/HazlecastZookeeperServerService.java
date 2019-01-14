/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.hazelcast.zookeeper.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.zookeeper.ZookeeperDiscoveryProperties;
import com.hazelcast.zookeeper.ZookeeperDiscoveryStrategyFactory;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

@Tags({ "HazleCast"})
@CapabilityDescription("Example ControllerService implementation of HazlecastService.")
public class HazlecastZookeeperServerService extends AbstractControllerService implements HazlecastService {

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
    private AtomicReference<HazelcastInstance> hazleCastServer = new AtomicReference<HazelcastInstance>();
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
        getLogger().info("Hazlecast Staring .. "+ clusterNAME);
        lock.lock();
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.setProperty(GroupProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        config.setInstanceName(clusterNAME);

        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(new ZookeeperDiscoveryStrategyFactory());
        discoveryStrategyConfig.addProperty(ZookeeperDiscoveryProperties.ZOOKEEPER_URL.key(), zookeeperURL);
        discoveryStrategyConfig.addProperty(ZookeeperDiscoveryProperties.ZOOKEEPER_PATH.key(), zookeeperPATH);
        discoveryStrategyConfig.addProperty(ZookeeperDiscoveryProperties.GROUP.key(), clusterNAME);
        config.getNetworkConfig().getJoin().getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);

        this.hazleCastServer.set(Hazelcast.getOrCreateHazelcastInstance(config));
        lock.unlock();

    }

    @OnDisabled
    public void shutdown() {

        getLogger().info("Hazlecast Shutting down.. "+ this.hazleCastServer.get());
        lock.lock();
        if(this.hazleCastServer.get() != null) this.hazleCastServer.get().shutdown();
        lock.unlock();

    }

    @Override
    public void execute() throws ProcessException {

    }

    @Override
    public void put(final String mapName, final Object key, final Object value)  throws ProcessException{}
    public Object get(final String mapName, final Object key)  throws ProcessException{ return null;}
}
