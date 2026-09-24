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

package org.apache.hugegraph.pd.pulse;

import java.util.function.Consumer;

import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;

public class PartitionNotice implements PulseServerNotice<PulseResponse> {
    private long noticeId;
    private Consumer<Long> ackConsumer;
    private PulseResponse content;

    public PartitionNotice(long noticeId, Consumer<Long> ackConsumer, PulseResponse content) {
        this.noticeId = noticeId;
        this.ackConsumer = ackConsumer;
        this.content = content;
    }

    @Override
    public void ack() {
        this.ackConsumer.accept(this.noticeId);
    }

    @Override
    public long getNoticeId() {
        return this.noticeId;
    }

    @Override
    public PulseResponse getContent() {
        return this.content;
    }
}
