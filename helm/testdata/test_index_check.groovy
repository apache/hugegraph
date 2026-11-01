// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Execute the rendered script, with stale cached ID 11 but healthy metadata.
class FixtureTraversal {
    def V() { this }
    def limit(count) { assert count == 1; this }
    def count() { 0 }
}

class FixtureGraph {
    Map statuses
    List lookups = []
    def schema() {
        return [getVertexLabels: { -> statuses.containsKey(11) ? [[indexLabels: { -> [11] }]] : [] },
                getEdgeLabels: { -> statuses.containsKey(22) ? [[indexLabels: { -> [22] }]] : [] },
                getIndexLabels: { -> throw new AssertionError('metadata enumeration refreshes cache') }]
    }
    def indexLabel(id) {
        assert id instanceof Integer : 'name lookup can refresh the cache'
        lookups.add(id)
        return [name: { -> "index_${id}" },
                status: { -> [name: { -> statuses[id] }, ok: { -> statuses[id] == 'CREATED' }] }]
    }
    def traversal() {
        return new FixtureTraversal()
    }
}

def source = new File(args[0]).text
[[11: 'CREATED', 22: 'CREATED'], [:]].each { statuses ->
    def graph = new FixtureGraph(statuses: statuses)
    assert new GroovyShell(new Binding(graph: graph)).evaluate(source) == 0
    assert graph.lookups as Set == statuses.keySet()
}
['CREATING', 'REBUILDING', 'DELETING', 'UNDELETED', 'INVALID', 'CLEARING'].each { status ->
    [11, 22].each { indexId ->
        def graph = new FixtureGraph(statuses: [11: 'CREATED', 22: 'CREATED'])
        graph.statuses[indexId] = status
        try {
            new GroovyShell(new Binding(graph: graph)).evaluate(source)
            throw new AssertionError('non-CREATED cached index must fail')
        } catch (IllegalStateException expected) {
            assert expected.message == "Index index_${indexId} is ${status} (CREATED expected)"
        }
    }
}
println 'cached index checks passed'
