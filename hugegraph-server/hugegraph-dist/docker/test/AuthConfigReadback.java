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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

import org.apache.hugegraph.auth.TokenGenerator;
import org.apache.hugegraph.config.AuthOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;

public final class AuthConfigReadback {

    public static void main(String[] args) {
        OptionSpace.register("auth", AuthOptions.instance());
        HugeConfig config = new HugeConfig(args[0]);
        String expected = new String(Base64.getDecoder().decode(args[1]), StandardCharsets.UTF_8);
        String actual = config.get(AuthOptions.AUTH_TOKEN_SECRET);
        if (!expected.equals(actual)) {
            throw new AssertionError("Signing key mismatch: expected characters=" + expected.length() +
                                     ", loaded characters=" + actual.length());
        }
        String token = new TokenGenerator(expected).create(Collections.singletonMap("probe", "auth"), 60000L);
        if (!"auth".equals(new TokenGenerator(actual).verify(token).get("probe"))) {
            throw new AssertionError("Signing key verification mismatch");
        }
        if (args.length == 3) {
            String admin = new String(Base64.getDecoder().decode(args[2]), StandardCharsets.UTF_8);
            if (!admin.equals(config.getProperty("auth.admin_pa"))) {
                throw new AssertionError("Administrator password mismatch");
            }
        }
        System.out.println("PASS: exact authentication property readback; signing key characters=" + actual.length());
    }
}
