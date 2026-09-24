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

package org.apache.hugegraph.store.query.concurrent;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class AtomicFloat extends Number implements Serializable, Comparable<AtomicFloat> {

    private static final AtomicIntegerFieldUpdater<AtomicFloat> FIELD_UPDATER;

    static {
        FIELD_UPDATER = AtomicIntegerFieldUpdater.newUpdater(AtomicFloat.class, "intBits");
    }

    private volatile int intBits;

    public AtomicFloat() {
        this.intBits = Float.floatToIntBits(0.0f);
    }

    public AtomicFloat(float value) {
        this.intBits = Float.floatToRawIntBits(value);
    }

    public float get() {
        return Float.intBitsToFloat(intBits);
    }

    public final void set(float newValue) {
        this.intBits = Float.floatToIntBits(newValue);
    }

    public final float getAndSet(float newValue) {
        return getAndSetFloat(newValue);
    }

    public final float getAndAdd(float delta) {
        return getAndAddFloat(delta);
    }

    /**
     * Adds the specified value to the current value and returns the sum
     *
     * @param delta The value to be added
     * @return Sum of current value and delta
     */
    public final float addAndGet(float delta) {
        return getAndAddFloat(delta) + delta;
    }

    /**
     * Compute and add floats. Appends the specified float delta to the current float and returns the sum
     *
     * @param delta The value to be added
     * @return Sum
     */
    private float getAndAddFloat(float delta) {
        int oldBits;
        int newBits;
        do {
            oldBits = intBits;
            newBits = Float.floatToIntBits(Float.intBitsToFloat(oldBits) + delta);
        } while (!FIELD_UPDATER.compareAndSet(this, oldBits, newBits));
        return Float.intBitsToFloat(oldBits);
    }

    /**
     * Set current float to the new one and return the old one
     *
     * @param newValue new float value
     * @return old value
     */
    private float getAndSetFloat(float newValue) {
        int oldBits;
        int newBits;
        do {
            oldBits = intBits;
            newBits = Float.floatToIntBits(newValue);
        } while (!FIELD_UPDATER.compareAndSet(this, oldBits, newBits));
        return Float.intBitsToFloat(oldBits);
    }

    /**
     * {@inheritDoc}
     * Cast value to int and return
     *
     * @return Int value
     */
    @Override
    public int intValue() {
        return (int) get();
    }

    /**
     * {@inheritDoc}
     * Cast to Long value and return
     *
     * @return Long value
     */
    @Override
    public long longValue() {
        return (long) get();
    }

    /**
     * {@inheritDoc} Return the current float value
     */
    @Override
    public float floatValue() {
        return get();
    }

    /**
     * {@inheritDoc}
     * Return double value
     *
     * @return current value in double type
     */
    @Override
    public double doubleValue() {
        return get();
    }

    /**
     * {@inheritDoc}
     * override method in super class, implement compareTo func
     *
     * @param o Value to compare
     * @return if current value less than o, return -1; if current value is greater than o,
     * return 1. Return 0 if equals
     */
    @Override
    public int compareTo(AtomicFloat o) {
        return Float.compare(get(), o.get());
    }

    /**
     * {@inheritDoc}
     * toString method
     *
     * @return A string containing integer bits (intBits) and the value
     */
    @Override
    public String toString() {
        return "AtomicFloat{" +
               "intBits=" + intBits +
               ", value = " + get() +
               '}';
    }
}
