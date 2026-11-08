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

package org.apache.hugegraph.perf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;

import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.ReflectionUtil;
import org.apache.hugegraph.func.TriFunction;
import org.apache.hugegraph.testutil.Assert.ThrowableConsumer;
import org.apache.hugegraph.perf.Stopwatch.Path;
import com.google.common.reflect.ClassPath.ClassInfo;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

public final class PerfUtil {

    private static final Logger LOG = Log.logger(PerfUtil.class);
    private static final int DEFAULT_CAPACITY = 1024;

    private static final ThreadLocal<PerfUtil> INSTANCE = new ThreadLocal<>();

    private static PerfUtil SINGLE_INSTANCE = null;
    private static Thread SINGLE_THREAD = null;
    private static LocalTimer LOCAL_TIMER = null;
    private static boolean LIGHT_WATCH = false;

    private final Map<Path, Stopwatch> stopwatches;
    private final LocalStack<Stopwatch> callStack;
    private final Stopwatch root;

    private PerfUtil() {
        this.stopwatches = new HashMap<>(DEFAULT_CAPACITY);
        this.callStack = new LocalStack<>(DEFAULT_CAPACITY);
        this.root = newStopwatch(Path.ROOT_NAME, Path.EMPTY);
    }

    public static PerfUtil instance() {
        if (SINGLE_INSTANCE != null &&
            SINGLE_THREAD == Thread.currentThread()) {
            // Return the only one instance for single thread, for performance
            return SINGLE_INSTANCE;
        }

        PerfUtil p = INSTANCE.get();
        if (p == null) {
            p = new PerfUtil();
            INSTANCE.set(p);
        }
        return p;
    }

    public static void profileSingleThread(boolean yes) {
        SINGLE_INSTANCE = yes ? PerfUtil.instance() : null;
        SINGLE_THREAD = yes ? Thread.currentThread() : null;
    }

    public static void useLocalTimer(boolean yes) {
        if (yes) {
            if (LOCAL_TIMER != null) {
                return;
            }
            LOCAL_TIMER = new LocalTimer();
            try {
                LOCAL_TIMER.startTimeUpdateLoop();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (!LIGHT_WATCH) {
                NormalStopwatch.initEachWastedLost();
            }
        } else {
            if (LOCAL_TIMER == null) {
                return;
            }
            try {
                LOCAL_TIMER.stop();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                LOCAL_TIMER = null;
            }
        }
    }

    public static void useLightStopwatch(boolean yes) {
        if (yes != LIGHT_WATCH) {
            PerfUtil instance = INSTANCE.get();
            boolean empty = instance == null || instance.empty();
            String message = "Please call clear() before switching " +
                             "light-stopwatch due to there is dirty watch";
            E.checkArgument(empty, message);
        }
        LIGHT_WATCH = yes;
    }

    protected static long now() {
        if (LOCAL_TIMER != null) {
            return LOCAL_TIMER.now();
        }
        // System.nanoTime() cost about 40 ns each call
        return System.nanoTime();
    }

    protected static Stopwatch newStopwatch(String name, Path parent) {
        return LIGHT_WATCH ? new LightStopwatch(name, parent) :
                             new NormalStopwatch(name, parent);
    }

    protected static Stopwatch newStopwatch(String name, Stopwatch parent) {
        return LIGHT_WATCH ? new LightStopwatch(name, parent) :
                             new NormalStopwatch(name, parent);
    }

    public Stopwatch start(String name) {
        long start = now();

        Stopwatch parent = this.callStack.empty() ?
                           this.root : this.callStack.peek();

        // Get watch by name from local tree
        Stopwatch watch = parent.child(name);
        if (watch == null) {
            watch = newStopwatch(name, parent);
            assert !this.stopwatches.containsKey(watch.id()) : watch;
            this.stopwatches.put(watch.id(), watch);
        }
        this.callStack.push(watch);

        watch.startTime(start);

        return watch;
    }

    public Stopwatch start2(String name) {
        long start = now(); // cost 70 ns with System.nanoTime()

        Path parent = this.callStack.empty() ?
                      Path.EMPTY : this.callStack.peek().id();
        Path id = Stopwatch.id(parent, name); // cost 130
        // Get watch by id from global map
        Stopwatch watch = this.stopwatches.get(id); // cost 170
        if (watch == null) {
            watch = newStopwatch(name, parent);
            this.stopwatches.put(watch.id(), watch); // cost 180
        }
        this.callStack.push(watch); // cost 190

        watch.startTime(start);

        return watch;
    }

    public void end(String name) {
        long start = LIGHT_WATCH ? 0L : now();

        Stopwatch watch = this.callStack.pop();
        if (watch == null || watch.name() != name) {
            throw new IllegalArgumentException("Invalid watch name: " + name);
        }

        watch.endTime(start);
    }

    public boolean empty() {
        return this.stopwatches.isEmpty() && this.root.empty();
    }

    public void clear() {
        String error = "Can't be cleared when the call has not ended yet";
        E.checkState(this.callStack.empty(), error);

        this.stopwatches.clear();
        this.root.clear();
    }

    public void profilePackage(String... packages) throws Throwable {
        Set<String> loadedClasses = new HashSet<>();

        Function<String, Boolean> inPackage = (cls) -> {
            for (String pkg : packages) {
                if (cls.startsWith(pkg)) {
                    return true;
                }
            }
            return false;
        };

        ThrowableConsumer<String> profileClassIfPresent = (cls) -> {
            if (!loadedClasses.contains(cls)) {
                // Profile super class
                for (String s : ReflectionUtil.superClasses(cls)) {
                    if (!loadedClasses.contains(s) && inPackage.apply(s)) {
                        profileClass(s);
                        loadedClasses.add(s);
                    }
                }
                // Profile self class
                profileClass(cls);
                loadedClasses.add(cls);
            }
        };

        Iterator<ClassInfo> classes = ReflectionUtil.classes(packages);
        while (classes.hasNext()) {
            String cls = classes.next().getName();
            // Profile self class
            profileClassIfPresent.accept(cls);
            // Profile nested class
            for (String s : ReflectionUtil.nestedClasses(cls)) {
                profileClassIfPresent.accept(s);
            }
        }
    }

    public void profileClass(String... classes) throws Throwable {
        ClassPool classPool = ClassPool.getDefault();

        for (String cls : classes) {
            CtClass ctClass = classPool.get(cls);
            List<CtMethod> methods = ReflectionUtil.getMethodsAnnotatedWith(
                                     ctClass, Watched.class, false);
            for (CtMethod method : methods) {
                profile(method);
            }

            // Load class and make it effective
            if (!methods.isEmpty()) {
                ctClass.toClass();
            }
        }
    }

    private void profile(CtMethod ctMethod)
                         throws CannotCompileException, ClassNotFoundException {
        final String START =
                "org.apache.hugegraph.perf.PerfUtil.instance().start(\"%s\");";
        final String END =
                "org.apache.hugegraph.perf.PerfUtil.instance().end(\"%s\");";

        Watched annotation = (Watched) ctMethod.getAnnotation(Watched.class);

        String name = annotation.value();
        if (name.isEmpty()) {
            name = ctMethod.getName();
        }
        if (!annotation.prefix().isEmpty()) {
            name = annotation.prefix() + "." + name;
        }

        ctMethod.insertBefore(String.format(START, name));
        // Insert as a finally-statement
        ctMethod.insertAfter(String.format(END, name), true);

        LOG.debug("Profiled for: '{}' [{}]", name, ctMethod.getLongName());
    }

    @Override
    public String toString() {
        return this.stopwatches.toString();
    }

    public String toJson() {
        StringBuilder sb = new StringBuilder(8 + this.stopwatches.size() * 96);
        sb.append('{');
        for (Map.Entry<Path, Stopwatch> w : this.stopwatches.entrySet()) {
            sb.append('"');
            sb.append(w.getKey());
            sb.append('"');

            sb.append(':');

            sb.append(w.getValue().toJson());

            sb.append(',');
        }
        if (!this.stopwatches.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append('}');
        return sb.toString();
    }

    // TODO: move toECharts() method out of this class
    public String toECharts() {
        TriFunction<Integer, Integer, List<Stopwatch>, String> formatLevel = (
                    totalDepth, depth, items) -> {
            float factor = 100.0f / (totalDepth + 1);
            float showFactor = 1 + (totalDepth - depth) / (float) depth;

            float radiusFrom = depth * factor;
            float radiusTo = depth * factor + factor;
            if (depth == 1) {
                radiusFrom = 0;
            }

            StringBuilder sb = new StringBuilder(8 + items.size() * 128);
            sb.append('{');
            sb.append("name: 'Total Cost',");
            sb.append("type: 'pie',");
            sb.append(String.format("radius: ['%s%%', '%s%%'],",
                                    radiusFrom, radiusTo));
            sb.append(String.format(
                      "label: {normal: {position: 'inner', formatter:" +
                      "function(params) {" +
                      "  if (params.percent > %s) return params.data.name;" +
                      "  else return '';" +
                      "}}},", showFactor));
            sb.append("data: [");

            items.sort((i, j) -> i.id().compareTo(j.id()));
            for (Stopwatch w : items) {
                sb.append('{');

                sb.append("id:'");
                sb.append(w.id());
                sb.append("',");

                sb.append("name:'");
                sb.append(w.name());
                sb.append("',");

                sb.append("value:");
                // w.totalCost() - w.totalWasted() ?
                sb.append(w.totalCost());
                sb.append(',');

                sb.append("cost:");
                sb.append(w.totalCost() / 1000000.0);
                sb.append(',');

                sb.append("minCost:");
                sb.append(w.minCost());
                sb.append(',');

                sb.append("maxCost:");
                sb.append(w.maxCost());
                sb.append(',');

                sb.append("wasted:");
                sb.append(w.totalWasted() / 1000000.0);
                sb.append(',');

                sb.append("selfWasted:");
                sb.append(w.totalSelfWasted() / 1000000.0);
                sb.append(',');

                sb.append("times:");
                sb.append(w.times());
                sb.append(',');

                sb.append("totalTimes:");
                sb.append(w.totalTimes());

                sb.append('}');
                sb.append(',');
            }
            if (!items.isEmpty()) {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append("]}");
            return sb.toString();
        };

        BiConsumer<List<Stopwatch>, List<Stopwatch>> fillChildrenTotal =
                (itemsOfLn, itemsOfLnParent) -> {
                    for (Stopwatch parent : itemsOfLnParent) {
                        List<Stopwatch> children = itemsOfLn.stream().filter(c -> {
                            return c.parent().equals(parent.id());
                        }).collect(Collectors.toList());

                        parent.fillChildrenTotal(children);
                    }
                };

        BiConsumer<List<Stopwatch>, List<Stopwatch>> fillOther =
                (itemsOfLn, itemsOfLnParent) -> {
                    for (Stopwatch parent : itemsOfLnParent) {
                        Stream<Stopwatch> children = itemsOfLn.stream().filter(c -> {
                            return c.parent().equals(parent.id());
                        });
                        // Fill other cost
                        long sumCost = children.mapToLong(Stopwatch::totalCost).sum();
                        long otherCost = parent.totalCost() - sumCost;
                        if (otherCost > 0L) {
                            Stopwatch other = newStopwatch("~", parent.id());
                            other.totalCost(otherCost);
                            itemsOfLn.add(other);
                        }
                    }
                };

        Map<Path, Stopwatch> items = this.stopwatches;
        Map<Integer, List<Stopwatch>> levelItems = new HashMap<>();
        int maxDepth = 0;
        for (Map.Entry<Path, Stopwatch> e : items.entrySet()) {
            int depth = e.getKey().toString().split("/").length;
            List<Stopwatch> levelItem = levelItems.get(depth);
            if (levelItem == null) {
                levelItem = new LinkedList<>();
                levelItems.putIfAbsent(depth, levelItem);
            }
            levelItem.add(e.getValue().copy());
            if (depth > maxDepth) {
                maxDepth = depth;
            }
        }

        // Fill wasted cost from the outermost to innermost
        for (int i = maxDepth; i > 0; i--) {
            assert levelItems.containsKey(i) : i;
            List<Stopwatch> itemsOfI = levelItems.get(i);
            List<Stopwatch> itemsOfParent = levelItems.get(i - 1);
            if (itemsOfParent != null) {
                // Fill total value of children
                fillChildrenTotal.accept(itemsOfI, itemsOfParent);
            }
        }

        StringBuilder sb = new StringBuilder(8 + items.size() * 128);
        // Output results header
        sb.append("{");
        sb.append("tooltip: {trigger: 'item', " +
            "formatter: function(params) {" +
            "  return params.data.name + ' ' + params.percent + '% <br/>'" +
            "    + 'cost: ' + params.data.cost + ' (ms) <br/>'" +
            "    + 'min cost: ' + params.data.minCost + ' (ns) <br/>'" +
            "    + 'max cost: ' + params.data.maxCost + ' (ns) <br/>'" +
            "    + 'wasted: ' + params.data.wasted + ' (ms) <br/>'" +
            "    + 'self wasted: ' + params.data.selfWasted + ' (ms) <br/>'" +
            "    + 'times: ' + params.data.times + '<br/>'" +
            "    + 'total times: ' + params.data.totalTimes + '<br/>'" +
            "    + 'path: ' + params.data.id + '<br/>';" +
            "}");
        sb.append("},");
        sb.append("series: [");
        // Output results data
        for (int i = 1; i <= maxDepth; i++) {
            assert levelItems.containsKey(i) : i;
            List<Stopwatch> itemsOfI = levelItems.get(i);
            List<Stopwatch> itemsOfParent = levelItems.get(i - 1);
            if (itemsOfParent != null) {
                // Fill other cost for non-root level, ignore root level (i=1)
                fillOther.accept(itemsOfI, itemsOfParent);
            }
            // Output items of level I
            sb.append(formatLevel.apply(maxDepth, i, itemsOfI));
            sb.append(',');
        }
        if (!items.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("]}");

        return sb.toString();
    }

    public static final class LocalTimer {

        // Header: 4 bytes classptr + 8 bytes markword
        private volatile long padding11 = 0L;
        private volatile long padding12 = 0L;
        private volatile long padding13 = 0L;
        private volatile long padding14 = 0L;
        private volatile long padding15 = 0L;
        private volatile long padding16 = 0L; // the 1st 64 bytes

        private volatile long time = 0L;

        private volatile long padding21 = 0L;
        private volatile long padding22 = 0L;
        private volatile long padding23 = 0L;
        private volatile long padding24 = 0L;
        private volatile long padding25 = 0L;
        private volatile long padding26 = 0L;
        private volatile long padding27 = 0L; // the 2nd 64 bytes

        private volatile boolean running = false;
        private Thread thread = null;

        public long now() {
            // Read current ns time (be called frequently)
            return this.time;
        }

        public void startTimeUpdateLoop() throws InterruptedException {
            assert this.thread == null;
            assert this.preventOptimizePadding() == 0L;
            this.running = true;
            CountDownLatch started = new CountDownLatch(1);
            this.thread = new Thread(() -> {
                started.countDown();
                while (this.running) {
                    this.time = System.nanoTime();
                    // Prevent frequent updates for perf (5.2s => 3.6s for 8kw)
                    Thread.yield();
                }
            }, "LocalTimer");
            this.thread.setDaemon(true);
            this.thread.start();
            started.await();
        }

        public void stop() throws InterruptedException {
            this.running = false;
            if (this.thread != null) {
                this.thread.join();
            }
        }

        public long preventOptimizePadding() {
            long p1 = this.padding11 + this.padding12 + this.padding13 +
                      this.padding14 + this.padding15 + this.padding16;
            long p2 = this.padding21 + this.padding22 + this.padding23 +
                      this.padding24 + this.padding25 + this.padding26 +
                      this.padding27;
            return p1 + p2;
        }
    }

    public static final class LocalStack<T> {

        private final Object[] elementData;
        private int elementCount;

        public LocalStack(int capacity) {
            this.elementData = new Object[capacity];
            this.elementCount = 0;
        }

        int size() {
            return this.elementCount;
        }

        boolean empty() {
            return this.elementCount == 0;
        }

        public void push(T elem) {
            this.elementData[this.elementCount++] = elem;
        }

        public T pop() {
            if (this.elementCount == 0) {
                throw new EmptyStackException();
            }
            this.elementCount--;
            @SuppressWarnings("unchecked")
            T elem = (T) this.elementData[this.elementCount];
            this.elementData[this.elementCount] = null;
            return elem;
        }

        public T peek() {
            if (this.elementCount == 0) {
                throw new EmptyStackException();
            }
            @SuppressWarnings("unchecked")
            T elem = (T) this.elementData[this.elementCount - 1];
            return elem;
        }
    }

    public static final class FastMap<K, V> {

        private final Map<K, V> hashMap;

        private K key1;
        private K key2;
        private K key3;

        private V val1;
        private V val2;
        private V val3;

        public FastMap() {
            this.hashMap = new HashMap<>();
        }

        public int size() {
            return this.hashMap.size();
        }

        public boolean containsKey(Object key) {
            return this.hashMap.containsKey(key);
        }

        public V get(Object key) {
            if (key == null) {
                return this.hashMap.get(null);
            }
            if (key == this.key1) {
                return this.val1;
            } else if (key == this.key2) {
                return this.val2;
            } else if (key == this.key3) {
                return this.val3;
            }

            return this.hashMap.get(key);
        }

        public V put(K key, V value) {
            if (key == null) {
                return this.hashMap.put(null, value);
            }
            // Update the slot already holding this key before taking a free one
            if (this.key1 != null && (key == this.key1 || key.equals(this.key1))) {
                this.val1 = value;
            } else if (this.key2 != null && (key == this.key2 || key.equals(this.key2))) {
                this.val2 = value;
            } else if (this.key3 != null && (key == this.key3 || key.equals(this.key3))) {
                this.val3 = value;
            } else if (this.key1 == null) {
                this.key1 = key;
                this.val1 = value;
            } else if (this.key2 == null) {
                this.key2 = key;
                this.val2 = value;
            } else if (this.key3 == null) {
                this.key3 = key;
                this.val3 = value;
            }

            return this.hashMap.put(key, value);
        }

        public V remove(Object key) {
            if (key == null) {
                return this.hashMap.remove(null);
            }
            if (this.key1 != null && (key == this.key1 || key.equals(this.key1))) {
                this.key1 = null;
                this.val1 = null;
            } else if (this.key2 != null && (key == this.key2 || key.equals(this.key2))) {
                this.key2 = null;
                this.val2 = null;
            } else if (this.key3 != null && (key == this.key3 || key.equals(this.key3))) {
                this.key3 = null;
                this.val3 = null;
            }

            return this.hashMap.remove(key);
        }

        public void clear() {
            this.key1 = null;
            this.key2 = null;
            this.key3 = null;

            this.val1 = null;
            this.val2 = null;
            this.val3 = null;

            this.hashMap.clear();
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.METHOD, ElementType.CONSTRUCTOR })
    public @interface Watched {
        String value() default "";
        String prefix() default "";
    }
}
