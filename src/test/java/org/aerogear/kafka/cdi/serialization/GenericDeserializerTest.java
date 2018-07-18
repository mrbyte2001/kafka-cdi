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
package org.aerogear.kafka.cdi.serialization;

import org.aerogear.kafka.serialization.GenericDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class GenericDeserializerTest {

    private  Deserializer<Object> objectDeserializer;
    private Deserializer<User> userDeserializer;

    @Before
    public void setup() {
        objectDeserializer = new GenericDeserializer<>(Object.class);
        userDeserializer = new GenericDeserializer<>(User.class);
    }


    @Test
    public void deserializeNull() {
        assertNull(objectDeserializer.deserialize("test-topic", null));
    }

    @Test(expected = org.apache.kafka.common.errors.SerializationException.class)
    public void deserializeEmpty() {
        assertNull(objectDeserializer.deserialize("test-topic", new byte[0]));
    }

    @Test(expected = org.apache.kafka.common.errors.SerializationException.class)
    public void deserializeWrongObject() {
        assertNull(userDeserializer.deserialize("test-topic", "{\"foo\":\"bar\"}".getBytes()));
    }

    @Test
    public void deserializeKey() {
        Map<String, Object> props = new HashMap<>();
        userDeserializer.configure(props, true);

        User octo = userDeserializer.deserialize("test-topic", "{\"username\":\"octo\", \"age\":\"21\"}".getBytes());

        assertNotNull(octo);
        assertEquals("octo", octo.getUsername());
        assertEquals(21, octo.getAge());
    }

    @Test
    public void deserializeValue() {
        HashMap<String, Object> props = new HashMap<>();
        userDeserializer.configure(props, false);

        User octo = userDeserializer.deserialize("test-topic", "{\"username\":\"octo\", \"age\":\"21\"}".getBytes());

        assertNotNull(octo);
        assertEquals("octo", octo.getUsername());
        assertEquals(21, octo.getAge());
    }


    public static class User {
        private String username;
        private int age;

        public User() {
        }

        public User(String username, int age) {
            this.username = username;
            this.age = age;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;

            final User other = (User) obj;
            return Objects.equals(this.username, other.username)
                    && Objects.equals(this.age, other.age);

        }

        public String getUsername() {
            return username;
        }

        public int getAge() {
            return age;
        }
    }
}