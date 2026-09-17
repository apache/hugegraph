/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit;

import org.apache.hugegraph.testutil.AssertTest;
import org.apache.hugegraph.testutil.WhiteboxTest;
import org.apache.hugegraph.unit.config.HugeConfigTest;
import org.apache.hugegraph.unit.config.OptionSpaceTest;
import org.apache.hugegraph.unit.event.EventHubTest;
import org.apache.hugegraph.unit.rest.AbstractRestClientTest;
import org.apache.hugegraph.unit.version.VersionTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import org.apache.hugegraph.unit.concurrent.AtomicLockTest;
import org.apache.hugegraph.unit.concurrent.BarrierEventTest;
import org.apache.hugegraph.unit.concurrent.KeyLockTest;
import org.apache.hugegraph.unit.concurrent.LockGroupTest;
import org.apache.hugegraph.unit.concurrent.LockManagerTest;
import org.apache.hugegraph.unit.concurrent.PausableScheduledThreadPoolTest;
import org.apache.hugegraph.unit.concurrent.RowLockTest;
import org.apache.hugegraph.unit.date.SafeDateFormatTest;
import org.apache.hugegraph.unit.iterator.BatchMapperIteratorTest;
import org.apache.hugegraph.unit.iterator.ExtendableIteratorTest;
import org.apache.hugegraph.unit.iterator.FilterIteratorTest;
import org.apache.hugegraph.unit.iterator.FlatMapperFilterIteratorTest;
import org.apache.hugegraph.unit.iterator.FlatMapperIteratorTest;
import org.apache.hugegraph.unit.iterator.LimitIteratorTest;
import org.apache.hugegraph.unit.iterator.ListIteratorTest;
import org.apache.hugegraph.unit.iterator.MapperIteratorTest;
import org.apache.hugegraph.unit.license.LicenseExtraParamTest;
import org.apache.hugegraph.unit.license.LicenseCreateParamTest;
import org.apache.hugegraph.unit.license.LicenseInstallParamTest;
import org.apache.hugegraph.unit.license.LicenseParamsTest;
import org.apache.hugegraph.unit.license.MachineInfoTest;
import org.apache.hugegraph.unit.perf.PerfUtilTest;
import org.apache.hugegraph.unit.perf.StopwatchTest;
import org.apache.hugegraph.unit.rest.RestClientTest;
import org.apache.hugegraph.unit.rest.RestResultTest;
import org.apache.hugegraph.unit.util.BytesTest;
import org.apache.hugegraph.unit.util.CollectionUtilTest;
import org.apache.hugegraph.unit.util.DateUtilTest;
import org.apache.hugegraph.unit.util.EcheckTest;
import org.apache.hugegraph.unit.util.HashUtilTest;
import org.apache.hugegraph.unit.util.InsertionOrderUtilTest;
import org.apache.hugegraph.unit.util.LogTest;
import org.apache.hugegraph.unit.util.LongEncodingGoldenTest;
import org.apache.hugegraph.unit.util.LongEncodingTest;
import org.apache.hugegraph.unit.util.NumericUtilTest;
import org.apache.hugegraph.unit.util.OrderLimitMapTest;
import org.apache.hugegraph.unit.util.ReflectionUtilTest;
import org.apache.hugegraph.unit.util.StringUtilTest;
import org.apache.hugegraph.unit.util.TimeUtilTest;
import org.apache.hugegraph.unit.util.UnitUtilTest;
import org.apache.hugegraph.unit.util.VersionUtilTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    LockManagerTest.class,
    LockGroupTest.class,
    AtomicLockTest.class,
    KeyLockTest.class,
    RowLockTest.class,
    PausableScheduledThreadPoolTest.class,

    HugeConfigTest.class,
    OptionSpaceTest.class,
    SafeDateFormatTest.class,
    BarrierEventTest.class,
    EventHubTest.class,
    PerfUtilTest.class,
    StopwatchTest.class,
    AbstractRestClientTest.class,
    RestClientTest.class,
    RestResultTest.class,
    VersionTest.class,

    ExtendableIteratorTest.class,
    FilterIteratorTest.class,
    LimitIteratorTest.class,
    MapperIteratorTest.class,
    FlatMapperIteratorTest.class,
    FlatMapperFilterIteratorTest.class,
    ListIteratorTest.class,
    BatchMapperIteratorTest.class,

    BytesTest.class,
    CollectionUtilTest.class,
    EcheckTest.class,
    HashUtilTest.class,
    InsertionOrderUtilTest.class,
    LogTest.class,
    NumericUtilTest.class,
    ReflectionUtilTest.class,
    StringUtilTest.class,
    TimeUtilTest.class,
    VersionUtilTest.class,
    LongEncodingTest.class,
    LongEncodingGoldenTest.class,
    OrderLimitMapTest.class,
    DateUtilTest.class,
    UnitUtilTest.class,

    LicenseExtraParamTest.class,
    LicenseCreateParamTest.class,
    LicenseInstallParamTest.class,
    LicenseParamsTest.class,
    MachineInfoTest.class,

    AssertTest.class,
    WhiteboxTest.class
})
public class UnitTestSuite {
}
