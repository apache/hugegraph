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

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;

/*
 * Test-only compatibility fixture for TinkerPop 3.7 merge steps. HugeGraph
 * currently compiles against 3.5, where these classes do not exist.
 */
public class MergeVertexStep extends TestMergeStep {

    public MergeVertexStep(Traversal.Admin<?, ?> traversal) {
        super(traversal);
    }
}

abstract class TestMergeStep extends AbstractStep<Object, Object>
                             implements TraversalParent {

    private final List<Traversal.Admin<?, ?>> children;

    TestMergeStep(Traversal.Admin<?, ?> traversal) {
        super(traversal);
        this.children = new ArrayList<>();
    }

    public void addChild(Traversal.Admin<?, ?> child) {
        this.children.add(child);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return (List) this.children;
    }

    @Override
    protected Traverser.Admin<Object> processNextStart()
                                                  throws NoSuchElementException {
        throw new NoSuchElementException();
    }
}
