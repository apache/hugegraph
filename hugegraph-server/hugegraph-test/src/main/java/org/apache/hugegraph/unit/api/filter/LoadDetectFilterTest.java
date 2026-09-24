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

package org.apache.hugegraph.unit.api.filter;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.api.filter.LoadDetectFilter;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.define.WorkLoad;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.inject.Provider;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.UriInfo;

public class LoadDetectFilterTest extends BaseUnitTest {

    private static final String TEST_LOGGER_NAME = LoadDetectFilter.class.getName();

    private LoadDetectFilter loadDetectFilter;
    private ContainerRequestContext requestContext;
    private UriInfo uriInfo;
    private WorkLoad workLoad;
    private TestAppender testAppender;
    private LoggerContext loggerContext;
    private org.apache.logging.log4j.core.config.Configuration loggerConfiguration;
    private LoggerConfig loggerConfig;

    @Before
    public void setup() {
        this.requestContext = Mockito.mock(ContainerRequestContext.class);
        this.uriInfo = Mockito.mock(UriInfo.class);
        this.workLoad = new WorkLoad();
        this.testAppender = new TestAppender();
        this.testAppender.start();
        this.loggerContext = (LoggerContext) LogManager.getContext(false);
        this.loggerConfiguration = this.loggerContext.getConfiguration();
        this.loggerConfig = new LoggerConfig(TEST_LOGGER_NAME, Level.WARN, false);
        this.loggerConfig.addAppender(this.testAppender, Level.WARN, null);
        this.loggerConfiguration.addLogger(TEST_LOGGER_NAME, this.loggerConfig);
        this.loggerContext.updateLoggers();

        Mockito.when(this.requestContext.getUriInfo()).thenReturn(this.uriInfo);
        Mockito.when(this.requestContext.getMethod()).thenReturn("GET");

        this.loadDetectFilter = new LoadDetectFilter();
        this.setLoadProvider(this.workLoad);
        this.setConfigProvider(createConfig(8, 0));
        this.setGcResults(false);
        this.setBusyLogPermits(true);
        this.setMemoryLogPermits(true);
        this.setFreeMemorySamples(1024L, 1024L);
    }

    @After
    public void teardown() {
        this.loggerConfiguration.removeLogger(TEST_LOGGER_NAME);
        this.loggerContext.updateLoggers();
        this.testAppender.stop();
    }

    @Test
    public void testFilter_WhiteListPathIgnored() {
        setupPath("", List.of(""));
        this.workLoad.incrementAndGet();

        this.loadDetectFilter.filter(this.requestContext);

        Assert.assertEquals(1, this.workLoad.get().get());
        Assert.assertTrue(this.testAppender.events().isEmpty());
    }

    @Test
    public void testFilter_RejectsWhenWorkerLoadIsTooHigh() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(2, 0));
        this.workLoad.incrementAndGet();

        ServiceUnavailableException exception = (ServiceUnavailableException) Assert.assertThrows(
                ServiceUnavailableException.class,
                () -> this.loadDetectFilter.filter(this.requestContext));

        Assert.assertContains("The server is too busy to process the request",
                              exception.getMessage());
        Assert.assertContains(ServerOptions.MAX_WORKER_THREADS.name(),
                              exception.getMessage());
        Assert.assertEquals(1, this.testAppender.events().size());
        this.assertWarnLogContains("Rejected request due to high worker load");
        this.assertWarnLogContains("method=GET");
        this.assertWarnLogContains("path=graphs/hugegraph/vertices");
        this.assertWarnLogContains("currentLoad=2");
    }

    @Test
    public void testFilter_RejectsWhenFreeMemoryIsTooLow() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(8, 512));
        this.setFreeMemorySamples(256L);
        this.setGcResults(false);

        ServiceUnavailableException exception = (ServiceUnavailableException) Assert.assertThrows(
                ServiceUnavailableException.class,
                () -> this.loadDetectFilter.filter(this.requestContext));

        Assert.assertContains("The server available memory 256(MB) is below than threshold 512(MB)",
                              exception.getMessage());
        Assert.assertEquals(1, this.testAppender.events().size());
        this.assertWarnLogContains("Rejected request due to low free memory");
        this.assertWarnLogContains("method=GET");
        this.assertWarnLogContains("path=graphs/hugegraph/vertices");
        this.assertWarnLogContains("presumableFreeMemMB=256");
        this.assertWarnLogContains("minFreeMemoryMB=512");
        this.assertWarnLogNotContains("recheckedFreeMemMB");
    }

    @Test
    public void testFilter_RejectsWhenFreeMemoryIsStillLowAfterGc() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(8, 512));
        this.setFreeMemorySamples(256L, 128L);
        this.setGcResults(true);

        ServiceUnavailableException exception = (ServiceUnavailableException) Assert.assertThrows(
                ServiceUnavailableException.class,
                () -> this.loadDetectFilter.filter(this.requestContext));

        Assert.assertContains("The server available memory 128(MB) is below than threshold 512(MB)",
                              exception.getMessage());
        Assert.assertEquals(1, this.testAppender.events().size());
        this.assertWarnLogContains("Rejected request due to low free memory after GC");
        this.assertWarnLogContains("presumableFreeMemMB=256");
        this.assertWarnLogContains("recheckedFreeMemMB=128");
        this.assertWarnLogContains("minFreeMemoryMB=512");
    }

    @Test
    public void testFilter_AllowsRequestWhenFreeMemoryRecoversAfterGc() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(8, 512));
        this.setFreeMemorySamples(256L, 1024L);
        this.setGcResults(true);

        this.loadDetectFilter.filter(this.requestContext);

        Assert.assertEquals(1, this.workLoad.get().get());
        Assert.assertEquals(1, this.testAppender.events().size());
        this.assertWarnLogContains("Low free memory recovered after GC");
        this.assertWarnLogContains("presumableFreeMemMB=256");
        this.assertWarnLogContains("recheckedFreeMemMB=1024");
        this.assertWarnLogContains("minFreeMemoryMB=512");
    }

    @Test
    public void testFilter_RejectsWhenFreeMemoryIsTooLowWithoutLogging() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(8, 512));
        this.setFreeMemorySamples(256L);
        this.setGcResults(false);
        this.setMemoryLogPermits(false);

        ServiceUnavailableException exception = (ServiceUnavailableException) Assert.assertThrows(
                ServiceUnavailableException.class,
                () -> this.loadDetectFilter.filter(this.requestContext));

        Assert.assertContains("The server available memory 256(MB) is below than threshold 512(MB)",
                              exception.getMessage());
        Assert.assertTrue(this.testAppender.events().isEmpty());
    }

    @Test
    public void testFilter_AllowsRequestWhenLoadAndMemoryAreHealthy() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(8, 0));

        this.loadDetectFilter.filter(this.requestContext);

        Assert.assertEquals(1, this.workLoad.get().get());
        Assert.assertTrue(this.testAppender.events().isEmpty());
    }

    @Test
    public void testFilter_RejectLogIsRateLimited() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(2, 0));
        this.setBusyLogPermits(true, false);

        this.workLoad.incrementAndGet();
        Assert.assertThrows(ServiceUnavailableException.class,
                            () -> this.loadDetectFilter.filter(this.requestContext));

        this.workLoad.get().set(1);
        Assert.assertThrows(ServiceUnavailableException.class,
                            () -> this.loadDetectFilter.filter(this.requestContext));

        Assert.assertEquals(1, this.testAppender.events().size());
        this.assertWarnLogContains("Rejected request due to high worker load");
    }

    @Test
    public void testFilter_BusyRejectLogPermitDoesNotAffectMemoryRejectLog() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(2, 0));
        this.setBusyLogPermits(false);
        this.workLoad.incrementAndGet();

        Assert.assertThrows(ServiceUnavailableException.class,
                            () -> this.loadDetectFilter.filter(this.requestContext));
        Assert.assertTrue(this.testAppender.events().isEmpty());

        this.workLoad.get().set(0);
        this.setConfigProvider(createConfig(8, 512));
        this.setFreeMemorySamples(256L);
        this.setGcResults(false);
        this.setMemoryLogPermits(true);

        Assert.assertThrows(ServiceUnavailableException.class,
                            () -> this.loadDetectFilter.filter(this.requestContext));

        Assert.assertEquals(1, this.testAppender.events().size());
        this.assertWarnLogContains("Rejected request due to low free memory");
    }

    @Test
    public void testFilter_MemoryRejectLogPermitDoesNotAffectBusyRejectLog() {
        setupPath("graphs/hugegraph/vertices",
                  List.of("graphs", "hugegraph", "vertices"));
        this.setConfigProvider(createConfig(8, 512));
        this.setFreeMemorySamples(256L);
        this.setGcResults(false);
        this.setMemoryLogPermits(false);

        Assert.assertThrows(ServiceUnavailableException.class,
                            () -> this.loadDetectFilter.filter(this.requestContext));
        Assert.assertTrue(this.testAppender.events().isEmpty());

        this.workLoad.get().set(1);
        this.setConfigProvider(createConfig(2, 0));
        this.setBusyLogPermits(true);

        Assert.assertThrows(ServiceUnavailableException.class,
                            () -> this.loadDetectFilter.filter(this.requestContext));

        Assert.assertEquals(1, this.testAppender.events().size());
        this.assertWarnLogContains("Rejected request due to high worker load");
    }

    private HugeConfig createConfig(int maxWorkerThreads, int minFreeMemory) {
        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(ServerOptions.MAX_WORKER_THREADS.name(), maxWorkerThreads);
        conf.setProperty(ServerOptions.MIN_FREE_MEMORY.name(), minFreeMemory);
        return new HugeConfig(conf);
    }

    private void setupPath(String path, List<String> segments) {
        List<PathSegment> pathSegments = segments.stream()
                                                 .map(this::createPathSegment)
                                                 .collect(Collectors.toList());
        Mockito.when(this.uriInfo.getPath()).thenReturn(path);
        Mockito.when(this.uriInfo.getPathSegments()).thenReturn(pathSegments);
    }

    private PathSegment createPathSegment(String path) {
        PathSegment segment = Mockito.mock(PathSegment.class);
        Mockito.when(segment.getPath()).thenReturn(path);
        return segment;
    }

    private void setLoadProvider(WorkLoad workLoad) {
        Whitebox.setInternalState(this.loadDetectFilter, "loadProvider",
                                  (Provider<WorkLoad>) () -> workLoad);
    }

    private void setConfigProvider(HugeConfig config) {
        Whitebox.setInternalState(this.loadDetectFilter, "configProvider",
                                  (Provider<HugeConfig>) () -> config);
    }

    private void setGcResults(boolean... gcResults) {
        this.setBooleanSupplier("gcTrigger", false, gcResults);
    }

    private void setBusyLogPermits(boolean... permits) {
        this.setBooleanSupplier("busyRejectLogPermit", true, permits);
    }

    private void setMemoryLogPermits(boolean... permits) {
        this.setBooleanSupplier("memoryRejectLogPermit", true, permits);
    }

    private void setFreeMemorySamples(long... freeMemorySamples) {
        Assert.assertTrue(freeMemorySamples.length > 0);
        Deque<Long> samples = new ArrayDeque<>();
        for (long freeMemorySample : freeMemorySamples) {
            samples.addLast(freeMemorySample);
        }
        long fallback = freeMemorySamples[freeMemorySamples.length - 1];
        Whitebox.setInternalState(this.loadDetectFilter, "freeMemorySupplier",
                                  (LongSupplier) () -> samples.isEmpty() ? fallback :
                                                     samples.removeFirst());
    }

    private void setBooleanSupplier(String fieldName, boolean fallback,
                                    boolean... values) {
        Deque<Boolean> results = new ArrayDeque<>();
        for (boolean value : values) {
            results.addLast(value);
        }
        Whitebox.setInternalState(this.loadDetectFilter, fieldName,
                                  (BooleanSupplier) () -> results.isEmpty() ?
                                                         fallback :
                                                         results.removeFirst());
    }

    private void assertWarnLogContains(String expectedContent) {
        Assert.assertFalse(this.testAppender.events().isEmpty());
        LogEvent event = this.testAppender.events().get(0);
        Assert.assertEquals(Level.WARN, event.getLevel());
        Assert.assertContains(expectedContent,
                              event.getMessage().getFormattedMessage());
    }

    private void assertWarnLogNotContains(String unexpectedContent) {
        Assert.assertFalse(this.testAppender.events().isEmpty());
        LogEvent event = this.testAppender.events().get(0);
        Assert.assertFalse(event.getMessage().getFormattedMessage()
                                .contains(unexpectedContent));
    }

    private static class TestAppender extends AbstractAppender {

        private final List<LogEvent> events;

        protected TestAppender() {
            super("LoadDetectFilterTestAppender", (Filter) null,
                  (Layout<? extends Serializable>) null, false,
                  Property.EMPTY_ARRAY);
            this.events = new ArrayList<>();
        }

        @Override
        public void append(LogEvent event) {
            this.events.add(event.toImmutable());
        }

        public List<LogEvent> events() {
            return this.events;
        }
    }
}
