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

package org.apache.hugegraph.traversal.optimize;

import java.util.NoSuchElementException;
import java.util.Objects;

import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;

public final class HugeCountStep<S extends Element>
        extends AbstractStep<S, Long> {

    private static final long serialVersionUID = -679873894532085972L;

    private final HugeGraphStep<?, S> originGraphStep;
    private boolean done = false;

    public HugeCountStep(final Traversal.Admin<?, ?> traversal,
                         final HugeGraphStep<?, S> originGraphStep) {
        super(traversal);
        E.checkNotNull(originGraphStep, "originGraphStep");
        this.originGraphStep = originGraphStep;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HugeCountStep)) {
            return false;
        }

        if (!super.equals(obj)) {
            return false;
        }

        HugeCountStep other = (HugeCountStep) obj;
        return Objects.equals(this.originGraphStep,
                              other.originGraphStep);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.originGraphStep);
    }

    @Override
    public void reset() {
        super.reset();
        this.done = false;
    }

    @Override
    protected Admin<Long> processNextStart() throws NoSuchElementException {
        if (this.done) {
            throw FastNoSuchElementException.instance();
        }
        this.done = true;
        @SuppressWarnings({"unchecked", "rawtypes"})
        Step<Long, Long> step = (Step) this;
        return this.getTraversal().getTraverserGenerator()
                   .generate(this.originGraphStep.count(), step, 1L);
    }
}
