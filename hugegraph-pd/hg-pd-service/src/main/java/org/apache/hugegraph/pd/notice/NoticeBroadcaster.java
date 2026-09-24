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

package org.apache.hugegraph.pd.notice;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.hugegraph.pd.common.HgAssert;

import lombok.extern.slf4j.Slf4j;

// TODO: merge/move to another package
@Slf4j
public class NoticeBroadcaster {

    private Supplier<Long> noticeSupplier;
    private long noticeId;
    private String durableId;
    private Supplier<String> durableSupplier;
    private Function<String, Boolean> removeFunction;
    private int state; //0=ready; 1=notified; 2=done ack; -1=error
    private int counter;
    private long timestamp;

    private NoticeBroadcaster(Supplier<Long> noticeSupplier) {
        this.noticeSupplier = noticeSupplier;
        this.timestamp = System.currentTimeMillis();
    }

    public static NoticeBroadcaster of(Supplier<Long> noticeSupplier) {
        HgAssert.isArgumentNotNull(noticeSupplier, "noticeSupplier");
        return new NoticeBroadcaster(noticeSupplier);
    }

    public NoticeBroadcaster setDurableSupplier(Supplier<String> durableSupplier) {
        this.durableSupplier = durableSupplier;
        return this;
    }

    public NoticeBroadcaster setRemoveFunction(Function<String, Boolean> removeFunction) {
        this.removeFunction = removeFunction;
        return this;
    }

    public NoticeBroadcaster notifying() {

        if (this.state >= 2) {
            log.warn("Aborted notifying as ack has done. notice: {}", this);
            return this;
        }

        this.counter++;

        if (this.durableId == null && this.durableSupplier != null) {
            try {
                this.durableId = this.durableSupplier.get();
            } catch (Throwable t) {
                log.error("Failed to invoke durableSupplier, cause by:", t);
            }
        }

        try {
            this.noticeId = this.noticeSupplier.get();
            state = 1;
        } catch (Throwable t) {
            state = -1;
            log.error("Failed to invoke noticeSupplier: {}; cause by: " +
                      this.noticeSupplier.toString(), t);
        }

        return this;
    }

    public boolean checkAck(long ackNoticeId) {
        boolean flag = false;

        if (this.noticeId == ackNoticeId) {
            flag = true;
            this.state = 2;
        }

        if (flag) {
            this.doRemoveDurable();
        }

        return flag;
    }

    public boolean doRemoveDurable() {
        log.info("Removing NoticeBroadcaster is stating, noticeId:{}, durableId: {}"
                , this.noticeId, this.durableId);
        boolean flag = false;

        if (this.removeFunction == null) {
            log.warn("The remove-function hasn't been set.");
            return false;
        }

        if (this.durableId == null) {
            log.warn("The durableId hasn't been set.");
            return false;
        }

        try {
            if (!(flag = this.removeFunction.apply(this.durableId))) {
                log.error("Removing NoticeBroadcaster was not complete, noticeId: {}, durableId: {}"
                        , this.noticeId, this.durableId);
            }
        } catch (Throwable t) {
            log.error("Failed to remove NoticeBroadcaster, noticeId: "
                      + this.noticeId + ", durableId: " + this.durableId + ". Cause by:", t);
        }

        return flag;
    }

    public long getNoticeId() {
        return noticeId;
    }

    public int getState() {
        return state;
    }

    public int getCounter() {
        return counter;
    }

    public String getDurableId() {
        return durableId;
    }

    public void setDurableId(String durableId) {

        if (HgAssert.isInvalid(durableId)) {
            log.warn("Set an invalid durable-id to NoticeBroadcaster.");
        }

        this.durableId = durableId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "NoticeBroadcaster{" +
               "noticeId=" + noticeId +
               ", durableId='" + durableId + '\'' +
               ", state=" + state +
               ", counter=" + counter +
               ", timestamp=" + timestamp +
               '}';
    }
}
