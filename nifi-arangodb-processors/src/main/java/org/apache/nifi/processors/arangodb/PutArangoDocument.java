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
package org.apache.nifi.processors.arangodb;

import com.arangodb.ArangoCollection;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;

@EventDriven
@Tags({"arangodb", "insert", "update", "replace", "write", "put"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Writes the contents of a FlowFile to ArangoDB")
public class PutArangoDocument extends AbstractArangoProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to ArangoDB are routed to this relationship").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to ArangoDB are routed to this relationship").build();

    static final String MODE_INSERT = "insert";
    static final String MODE_UPDATE = "update";
    static final String MODE_REPLACE = "replace";

    static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("Mode")
            .description("Indicates whether the processor should insert, update, or replace")
            .required(true)
            .allowableValues(MODE_INSERT, MODE_UPDATE, MODE_REPLACE)
            .defaultValue(MODE_INSERT)
            .build();
    static final PropertyDescriptor INSERT_QUERY_KEY = new PropertyDescriptor.Builder()
            .name("Update Query Key")
            .description("Key name used to build the update query criteria; this property is valid only when using insert mode, "
                    + "otherwise it is ignored")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("_id")
            .build();
    static final PropertyDescriptor UPDATE_QUERY_KEY = new PropertyDescriptor.Builder()
            .name("Update or Replace Query Key")
            .description("Key name used to build the update query criteria; this property is valid only when using update or replace mode, "
                    + "otherwise it is ignored")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("_key")
            .build();
    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the data is encoded")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(MODE);
        _propertyDescriptors.add(UPDATE_QUERY_KEY);
        _propertyDescriptors.add(INSERT_QUERY_KEY);
        _propertyDescriptors.add(CHARACTER_SET);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }


    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

        final FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final Charset charset = Charset.forName(processContext.getProperty(CHARACTER_SET).getValue());
        final ArangoCollection collection = getCollection(processContext);
        final String mode = processContext.getProperty(MODE).getValue();

        try {
            final byte[] content = new byte[(int) flowFile.getSize()];
            processSession.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });

            if (MODE_INSERT.equalsIgnoreCase(mode)) {
                collection.insertDocument(new String(content, charset));
                logger.info("inserted {} into ArangoDB", new Object[]{flowFile});
            } else if (MODE_UPDATE.equalsIgnoreCase(mode)) {
                final String updateKey = processContext.getProperty(UPDATE_QUERY_KEY).getValue();
                collection.updateDocument(updateKey, new String(content, charset));
                logger.info("updated {} into ArangoDB", new Object[]{flowFile});
            } else {
                final String updateKey = processContext.getProperty(UPDATE_QUERY_KEY).getValue();
                collection.replaceDocument(updateKey, new String(content, charset));
                logger.info("replaced {} into ArangoDB", new Object[]{flowFile});
            }
            processSession.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            logger.error("Failed to insert {} into ArangoDB due to {}", new Object[]{flowFile, e}, e);
            processSession.transfer(flowFile, REL_FAILURE);
            processContext.yield();
        }
    }
}
