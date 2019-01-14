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
package org.apache.nifi.processors.hazelcast.processors;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hazelcast.zookeeper.service.HazlecastService;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@EventDriven
//@SupportsBatching
public class GetHazlecastMap extends AbstractProcessor {

    public static final PropertyDescriptor HAZLECASTZOOKEEPERCLIENTSERVICE = new PropertyDescriptor
            .Builder().name("HazlecastZookeeperClientService")
            .displayName("HazlecastZookeeperClientService")
            .description("Hazlecast ZookeeperClientService")
            .required(true)
            .identifiesControllerService(HazlecastService.class)
            .build();

    public static final PropertyDescriptor MAP_NAME = new PropertyDescriptor
            .Builder().name("MAP_NAME")
            .displayName("Map Name")
            .description("Hazlecast Map name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("ORIGINAL")
            .description("ORIGINAL relationship")
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure relationship")
            .build();

    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of FlowFiles to process in a single execution.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("25")
            .build();
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    protected HazlecastService clientService;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientService = context.getProperty(HAZLECASTZOOKEEPERCLIENTSERVICE).asControllerService(HazlecastService.class);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(HAZLECASTZOOKEEPERCLIENTSERVICE);
        descriptors.add(MAP_NAME);
        descriptors.add(BATCH_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        FlowFile flowFile = session.get();
        if (flowFile == null ) {
            return;
        }

        final   PutFlowFile putFlowFile = processFlowFile(session,context,flowFile);
        if (putFlowFile.getSuccess()) {
            // sub-classes should log appropriate error messages before returning null
            session.transfer(putFlowFile.getFlowFile(), REL_SUCCESS);
        } else {
            session.transfer(putFlowFile.getFlowFile(), REL_FAILURE);
        }
        session.transfer(flowFile,REL_ORIGINAL);

    }

    private PutFlowFile processFlowFile(final ProcessSession session, final ProcessContext context, final FlowFile flowFile){

        final ObjectMapper mapper = new ObjectMapper();

        final AtomicReference<Map> jsonDataIn = new AtomicReference<>(null);
        session.read(flowFile, in -> {
            try (final InputStream bufferedIn = new BufferedInputStream(in)) {

                jsonDataIn.set(mapper.readValue(bufferedIn,HashMap.class));
            }
        });
        final String value = clientService.get(MAP_NAME.getName(), jsonDataIn.get().get("key")).toString();
        FlowFile retturnFlowFile = session.create();

        PutFlowFile putFlowFile = new PutFlowFile();
        putFlowFile.setFlowFile(retturnFlowFile);
        try{
            // write the row to a new FlowFile.
            retturnFlowFile = session.write(retturnFlowFile, out -> out.write(value.getBytes(StandardCharsets.UTF_8)));
            putFlowFile.setSuccess(Boolean.TRUE);
            return putFlowFile;

        } catch (final Exception pe) {
            getLogger().error("Failed to parse {} as JSON due to {}; routing to failure", new Object[]{flowFile, pe.toString()}, pe);
            putFlowFile.setSuccess(Boolean.FALSE);
            return putFlowFile;
        }

//        if (rootNode.isArray()) {
//            getLogger().error("Root node of JSON must be a single document, found array for {}; routing to failure", new Object[]{flowFile});
//            return null;
//        }



    }
}
