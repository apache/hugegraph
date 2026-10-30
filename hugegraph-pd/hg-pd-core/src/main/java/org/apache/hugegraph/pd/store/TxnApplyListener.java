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

package org.apache.hugegraph.pd.store;

/**
 * Hands a committed graph record change to schema sync dispatch. Called on the raft apply
 * thread of every node, right after the write batch of a successful TXN that wrote a record,
 * never for a failed TXN. It must not block. What it throws is logged and dropped: the TXN is
 * committed whatever the listener does.
 * <p>
 * A subscriber must take repeats. On a restart after a snapshot at index S, the store is
 * replaced by the snapshot and the TXNs after S are applied again, so the listener sees them
 * again with the same index and revision; on a restart without a snapshot the same TXNs are
 * skipped and the listener is not called. On the path without raft (raft.enable=false) it
 * runs on the gRPC thread of the txn call, under the store's write lock, and {@code index} is
 * the store's own TXN count, not a raft log index.
 */
public interface TxnApplyListener {

    /**
     * @param index       the raft log index of the TXN (the local count without raft)
     * @param recordKey   the key of the graph record
     * @param revision    the record's new revision: index, or the previous revision plus
     *                    one when that is higher, so it only rises
     * @param incarnation the record's incarnation after the TXN
     * @param state       LIVE or DROPPED
     */
    void onTxnApplied(long index, String recordKey, long revision, long incarnation,
                      String state);
}
