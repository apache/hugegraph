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

package org.apache.hugegraph.store.term;

import java.io.Serializable;
import java.util.Objects;

public class HgPair<K, V> implements Serializable {

    /**
     * This is the key associated with this <code>Pair</code>.
     */
    private K key;

    /**
     * This is the value associated with this <code>Pair</code>.
     */
    private V value;

    public HgPair() {

    }

    /**
     * Initializes a new pair with the specified key and value.
     *
     * @param key   The key to be associated with this pair
     * @param value The value to be associated with this pair
     */
    public HgPair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Retrieves the key associated with this pair.
     *
     * @return the key of this pair
     */
    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    /**
     * Retrieves the value associated with this pair.
     *
     * @return the value of this pair
     */
    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    /**
     * Provides a <code>String</code> representation of this <code>Pair</code>.
     *
     * <p>The default delimiter between name and value is '='.</p>
     *
     * @return a <code>String</code> representation of this <code>Pair</code>
     */
    @Override
    public String toString() {
        return key + "=" + value;
    }

    /**
     * Generates a hash code for this <code>Pair</code>.
     *
     * <p>The hash code is computed using both the key and
     * the value of the <code>Pair</code>.</p>
     *
     * @return the hash code for this <code>Pair</code>
     */
    @Override
    public int hashCode() {
        // The hashCode of the key is multiplied by a prime number (13)
        // to ensure uniqueness between different key-value combinations:
        //  key: a  value: aa
        //  key: aa value: a
        return key.hashCode() * 13 + (value == null ? 0 : value.hashCode());
    }

    /**
     * Checks if this <code>Pair</code> is equal to another <code>Object</code>.
     *
     * <p>This method returns <code>false</code> if the tested
     * <code>Object</code> is not a <code>Pair</code> or is <code>null</code>.</p>
     *
     * <p>Two <code>Pair</code>s are equal if their keys and values are both equal.</p>
     *
     * @param o the <code>Object</code> to compare with this <code>Pair</code>
     * @return <code>true</code> if the specified <code>Object</code> is
     * equal to this <code>Pair</code>, otherwise <code>false</code>
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof HgPair<?, ?> pair) {
            return Objects.equals(key, pair.key) && Objects.equals(value, pair.value);
        }
        return false;
    }
}
