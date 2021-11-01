/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams;

import org.apache.kafka.common.KafkaException;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Base64;

public final class OffsetVectorFactory {

    private OffsetVectorFactory() {

    }

    public static OffsetVector createOffsetVector() {
        return new OffsetVectorImpl();
    }

    public static OffsetVector deserialize(final String token) {
        final byte[] data = Base64.getDecoder().decode(token);
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data))) {
            final OffsetVector ct = (OffsetVector) in.readObject();
            return ct;
        } catch (final Exception e) {
            throw new KafkaException("Couldn't decode consistency token", e);
        }
    }
}



