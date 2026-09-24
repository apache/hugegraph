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

package org.apache.hugegraph.store.rocksdb;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.junit.Test;

public class RocksDBFactoryTest extends BaseRocksDbTest {

    @Test
    public void testCreateSession() throws NoSuchMethodException, InvocationTargetException,
                                           InstantiationException, IllegalAccessException {
        Constructor<RocksDBFactory> constructor =
                RocksDBFactory.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        RocksDBFactory factory = constructor.newInstance();
        factory.setHugeConfig(hConfig);

        try (RocksDBSession dbSession = factory.createGraphDB("./tmp", "test1")) {
            SessionOperator op = dbSession.sessionOp();
            op.prepare();
            try {
                op.put("tbl", "k1".getBytes(), "v1".getBytes());
                op.commit();
            } catch (Exception e) {
                op.rollback();
            }

        }
        factory.destroyGraphDB("test1");
    }

    @Test
    public void testTotalKeys() {
        RocksDBFactory dbFactory = RocksDBFactory.getInstance();
        System.out.println(dbFactory.getTotalSize());

        System.out.println(dbFactory.getTotalKey().entrySet()
                                    .stream().map(e -> e.getValue()).reduce(0L, Long::sum));
    }

    @Test
    public void releaseAllGraphDB() {
        System.out.println(RocksDBFactory.class);

        RocksDBFactory rFactory = RocksDBFactory.getInstance();

        if (rFactory.queryGraphDB("bj01") == null) {
            rFactory.createGraphDB("./tmp", "bj01");
        }

        if (rFactory.queryGraphDB("bj02") == null) {
            rFactory.createGraphDB("./tmp", "bj02");
        }

        if (rFactory.queryGraphDB("bj03") == null) {
            rFactory.createGraphDB("./tmp", "bj03");
        }

        RocksDBSession dbSession = rFactory.queryGraphDB("bj01");

        dbSession.checkTable("test");
        SessionOperator sessionOp = dbSession.sessionOp();
        sessionOp.prepare();

        sessionOp.put("test", "hi".getBytes(), "byebye".getBytes());
        sessionOp.commit();

        rFactory.releaseAllGraphDB();
    }
}
