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

package org.apache.hugegraph.backend.tx;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.exception.BackendException;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStore;

public abstract class IndexableTransaction extends AbstractTransaction {

    public IndexableTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);
    }

    @Override
    public boolean hasUpdate() {
        AbstractTransaction indexTx = this.indexTransaction();
        boolean indexTxChanged = (indexTx != null && indexTx.hasUpdate());
        return indexTxChanged || super.hasUpdate();
    }

    @Override
    protected void reset() {
        super.reset();

        // It's null when called by super AbstractTransaction()
        AbstractTransaction indexTx = this.indexTransaction();
        if (indexTx != null) {
            indexTx.reset();
        }
    }

    @Override
    protected void commit2Backend() {
        BackendMutation mutation = this.prepareCommit();
        BackendMutation idxMutation = this.indexTransaction().prepareCommit();
        assert !mutation.isEmpty() || !idxMutation.isEmpty();
        // Commit graph/schema updates and index updates with graph/schema tx
        this.commitMutation2Backend(mutation, idxMutation);
    }

    @Override
    public void commitIfGtSize(int size) throws BackendException {
        int totalSize = this.mutationSize() +
                        this.indexTransaction().mutationSize();
        if (totalSize >= size) {
            this.commit();
        }
    }

    @Override
    public void rollback() throws BackendException {
        try {
            super.rollback();
        } finally {
            this.indexTransaction().rollback();
        }
    }

    @Override
    public void close() {
        try {
            this.indexTransaction().close();
        } finally {
            super.close();
        }
    }

    protected abstract AbstractTransaction indexTransaction();
}
