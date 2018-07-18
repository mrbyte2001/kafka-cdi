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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.aerogear.kafka.serialization.GenericSerializer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class GenericSerializerTest {

    private ObjectMapper objectMapper = new ObjectMapper();
    private GenericSerializer<Object> serializer;

    @Before
    public void setup() {
        serializer = new GenericSerializer<>();
    }

    @Test
    public void serializeNull() {
        assertNull(serializer.serialize("test-topic", null));
    }

    @Test
    public void serialize() throws IOException {
        Map<String, Object> message = new HashMap<>();
        message.put("foo", "bar");
        message.put("baz", 354.99);

        byte[] bytes = serializer.serialize("test-topic", message);

        Object deserialized = this.objectMapper.readValue(bytes, Object.class);
        assertEquals(message, deserialized);
    }

    @Test
    public void serializeUser() throws IOException {
        User user = new User("foo", 21);
        byte[] bytes = serializer.serialize("test-topic", user);

        assertEquals(user, this.objectMapper.readValue(bytes, User.class));
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