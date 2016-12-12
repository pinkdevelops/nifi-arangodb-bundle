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
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractArangoProcessor extends AbstractProcessor {

    protected static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("ArangoDB host")
            .description("ArangoDB host, defaults to 127.0.0.1")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("127.0.0.1")
            .build();
    protected static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("ArangoDB port")
            .description("ArangoDB port, defaults to 8529")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("8529")
            .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username to access ArangoDB")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password to access ArangoDB")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    protected static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("Arango Database Name")
            .description("The name of the database to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
            .name("Arango Collection Name")
            .description("The name of the collection to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor USE_SSL = new PropertyDescriptor.Builder()
            .name("use SSL connection")
            .description("The name of the collection to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(DATABASE_NAME);
        descriptors.add(COLLECTION_NAME);
        descriptors.add(HOST);
        descriptors.add(PORT);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
    }

    protected ArangoDB arangoDB;

    @OnScheduled
    public final void createArango(ProcessContext context) throws IOException {
        if (arangoDB != null) {
            closeArango();
        }

        final String host = context.getProperty(HOST).getValue();
        final int port = context.getProperty(PORT).asInteger();
        final String user = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();

        arangoDB = new ArangoDB.Builder().host(host).port(port).user(user).password(password).build();
        getLogger().info("Creating ArangoDB Connection");
    }

    @OnStopped
    public final void closeArango() {
        if (arangoDB != null) {
            getLogger().info("Closing ArangoDB");
            arangoDB.shutdown();
            arangoDB = null;
        }
    }

    protected ArangoDatabase getDatabase(final ProcessContext context) {
        final String databaseName = context.getProperty(DATABASE_NAME).getValue();
        return arangoDB.db(databaseName);
    }

    protected ArangoCollection getCollection(final ProcessContext context) {
        final String collectionName = context.getProperty(COLLECTION_NAME).getValue();
        return getDatabase(context).collection(collectionName);
    }

}
