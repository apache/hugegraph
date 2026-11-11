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

package org.apache.hugegraph.define;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.NumericUtil;

import com.google.common.collect.Sets;

public enum UpdateStrategy {

    // Only number support sum
    SUM {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            // TODO: Improve performance? (like write a method in common module)
            BigDecimal oldNumber = new BigDecimal(oldProperty.toString());
            BigDecimal newNumber = new BigDecimal(newProperty.toString());
            return oldNumber.add(newNumber);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            E.checkArgument(oldProperty instanceof Number &&
                            newProperty instanceof Number,
                            this.formatError(oldProperty, newProperty,
                                             "Number"));
        }
    },

    // Only Date & Number support compare
    BIGGER {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return compareNumber(oldProperty, newProperty, BIGGER);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            E.checkArgument((oldProperty instanceof Date ||
                             oldProperty instanceof Number) &&
                            (newProperty instanceof Date ||
                             newProperty instanceof Number),
                            this.formatError(oldProperty, newProperty,
                                             "Date or Number"));
        }
    },

    SMALLER {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return compareNumber(oldProperty, newProperty, SMALLER);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            E.checkArgument((oldProperty instanceof Date ||
                             oldProperty instanceof Number) &&
                            (newProperty instanceof Date ||
                             newProperty instanceof Number),
                            this.formatError(oldProperty, newProperty,
                                             "Date or Number"));
        }
    },

    // Only Set support union & intersection
    UNION {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return combineSet(oldProperty, newProperty, UNION);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            // JsonElements are always List-type, so allows two type now.
            this.checkCollectionType(oldProperty, newProperty);
        }
    },

    INTERSECTION {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return combineSet(oldProperty, newProperty, INTERSECTION);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            this.checkCollectionType(oldProperty, newProperty);
        }
    },

    // Batch update Set should use union because of higher efficiency
    APPEND {
        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            ((Collection) oldProperty).addAll((Collection) newProperty);
            return oldProperty;
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            this.checkCollectionType(oldProperty, newProperty);
        }
    },

    ELIMINATE {
        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            ((Collection) oldProperty).removeAll((Collection) newProperty);
            return oldProperty;
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            this.checkCollectionType(oldProperty, newProperty);
        }
    },

    OVERRIDE {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return newProperty;
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            // Allow any type
        }
    };

    abstract Object updatePropertyValue(Object oldProperty, Object newProperty);

    abstract void checkPropertyType(Object oldProperty, Object newProperty);

    public Object checkAndUpdateProperty(Object oldProperty,
                                         Object newProperty) {
        this.checkPropertyType(oldProperty, newProperty);
        return this.updatePropertyValue(oldProperty, newProperty);
    }

    protected String formatError(Object oldProperty, Object newProperty,
                                 String className) {
        return String.format("Property type must be %s for strategy %s, " +
                             "but got type %s, %s", className, this,
                             oldProperty.getClass().getSimpleName(),
                             newProperty.getClass().getSimpleName());
    }

    protected void checkCollectionType(Object oldProperty,
                                       Object newProperty) {
        E.checkArgument((oldProperty instanceof Set ||
                         oldProperty instanceof List) &&
                        (newProperty instanceof Set ||
                         newProperty instanceof List),
                        this.formatError(oldProperty, newProperty,
                                         "Set or List"));
    }

    protected static Object compareNumber(Object oldProperty,
                                          Object newProperty,
                                          UpdateStrategy strategy) {
        Number oldNum = NumericUtil.convertToNumber(oldProperty);
        Number newNum = NumericUtil.convertToNumber(newProperty);
        int result = NumericUtil.compareNumber(oldNum, newNum);
        return strategy == BIGGER ? (result > 0 ? oldProperty : newProperty) :
               (result < 0 ? oldProperty : newProperty);
    }

    protected static Set<?> combineSet(Object oldProperty, Object newProperty,
                                       UpdateStrategy strategy) {
        Set<?> oldSet = oldProperty instanceof Set<?> oldValues ?
                        oldValues :
                        new HashSet<>((List<?>) oldProperty);
        Set<?> newSet = newProperty instanceof Set<?> newValues ?
                        newValues :
                        new HashSet<>((List<?>) newProperty);
        return strategy == UNION ? Sets.union(oldSet, newSet) :
               Sets.intersection(oldSet, newSet);
    }
}
