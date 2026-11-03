#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Regression coverage for the optional HugeGraph RAG Helm component."""

import unittest

try:
    from rag_tests.basic import RagBasicMixin
    from rag_tests.state import RagStateMixin
    from rag_tests.support import RagChartBase
    from rag_tests.validation import RagValidationMixin
    from rag_tests.wiring import RagWiringMixin
except ModuleNotFoundError as error:
    if error.name != "rag_tests":
        raise
    from helm.rag_tests.basic import RagBasicMixin
    from helm.rag_tests.state import RagStateMixin
    from helm.rag_tests.support import RagChartBase
    from helm.rag_tests.validation import RagValidationMixin
    from helm.rag_tests.wiring import RagWiringMixin


class RagChartTest(
    RagBasicMixin,
    RagWiringMixin,
    RagStateMixin,
    RagValidationMixin,
    RagChartBase,
):
    """Rendered Helm contract assembled from focused behavior mixins."""


if __name__ == "__main__":
    unittest.main()
