/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hugegraph.util.LongEncoding;

/**
 * Generates the golden fixture corpus for {@link LongEncoding}, see
 * {@link LongEncodingGoldenTest} for the format and the replay side.
 *
 * The expected results are recorded by running the current implementation,
 * not derived by hand: for every input the generator executes the method
 * and records either the returned value or the raised exception class.
 *
 * Regenerate (from the hugegraph-commons directory):
 *   mvn -pl hugegraph-common test-compile dependency:build-classpath \
 *       -Dmdep.outputFile=target/cp.txt -q
 *   java -cp "hugegraph-common/target/test-classes:hugegraph-common/target/classes:$(cat hugegraph-common/target/cp.txt)" \
 *       org.apache.hugegraph.unit.util.LongEncodingGoldenGenerator \
 *       hugegraph-common/src/test/resources/longencoding-golden.txt
 *
 * The corpus is deterministic: fixed value sets plus Random with a fixed
 * seed, so regenerating produces the same file as long as the encoding
 * behavior is unchanged.
 */
public final class LongEncodingGoldenGenerator {

    private static final long SEED = 3145L;
    private static final int RANDOM_VALUES_PER_BIT_LENGTH = 4;

    private LongEncodingGoldenGenerator() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: LongEncodingGoldenGenerator <output-file>");
            System.exit(1);
        }
        OutputStream output = Files.newOutputStream(Paths.get(args[0]));
        try (Writer writer = new OutputStreamWriter(output,
                                                    StandardCharsets.US_ASCII)) {
            generate(writer);
        }
        System.out.println("Corpus written to " + args[0]);
    }

    static void generate(Writer writer) throws IOException {
        writeHeader(writer);
        Emitter emitter = new Emitter(writer);

        for (long value : longValues()) {
            recordLongOp(emitter, "encodeSortable", value);
            recordRoundTrip(emitter, "decodeSortable",
                            LongEncoding.encodeSortable(value), value);
            recordLongOp(emitter, "encodeSignedB64", value);
            recordRoundTrip(emitter, "decodeSignedB64",
                            LongEncoding.encodeSignedB64(value), value);
            recordLongOp(emitter, "encodeB64", value);
            if (value >= 0L) {
                recordRoundTrip(emitter, "decodeB64",
                                LongEncoding.encodeB64(value), value);
            }
        }

        for (String input : decodeInputs()) {
            recordStringOp(emitter, "decodeSortable", input);
            recordStringOp(emitter, "decodeSignedB64", input);
            recordStringOp(emitter, "decodeB64", input);
        }
    }

    /**
     * Writes corpus lines, silently dropping exact duplicates: the hand
     * picked decode inputs overlap with the length sweep and with the
     * round trips derived from the value set.
     */
    private static final class Emitter {

        private final Writer writer;
        private final Set<String> seen = new HashSet<>();

        Emitter(Writer writer) {
            this.writer = writer;
        }

        void line(String method, String arg, String expected)
                  throws IOException {
            String line = method + '\t' + arg + '\t' + expected;
            if (this.seen.add(line)) {
                this.writer.write(line);
                this.writer.write('\n');
            }
        }
    }

    private static void writeHeader(Writer writer) throws IOException {
        String[] lines = {
            "Licensed to the Apache Software Foundation (ASF) under one or more",
            "contributor license agreements. See the NOTICE file distributed with this",
            "work for additional information regarding copyright ownership. The ASF",
            "licenses this file to You under the Apache License, Version 2.0 (the",
            "\"License\"); you may not use this file except in compliance with the License.",
            "You may obtain a copy of the License at",
            "",
            "    http://www.apache.org/licenses/LICENSE-2.0",
            "",
            "Unless required by applicable law or agreed to in writing, software",
            "distributed under the License is distributed on an \"AS IS\" BASIS, WITHOUT",
            "WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the",
            "License for the specific language governing permissions and limitations",
            "under the License.",
            "",
            "Golden fixture corpus for org.apache.hugegraph.util.LongEncoding.",
            "Generated by LongEncodingGoldenGenerator, do not edit by hand.",
            "",
            "Format: <method>\\t<argument>\\t<expected>",
            "  long arguments are decimal",
            "  string arguments are escaped: \\t \\n \\r \\\\, \\uXXXX outside",
            "  printable ASCII, and the token \\N alone means a null reference",
            "  expected is ok:<value> or throw:<exception simple class name>"
        };
        for (String line : lines) {
            writer.write(line.isEmpty() ? "#\n" : "# " + line + "\n");
        }
    }

    private static TreeSet<Long> longValues() {
        TreeSet<Long> values = new TreeSet<>();
        for (long v = -16L; v <= 16L; v++) {
            values.add(v);
        }
        values.add(Long.MIN_VALUE);
        values.add(Long.MIN_VALUE + 1L);
        values.add(Long.MAX_VALUE);
        values.add(Long.MAX_VALUE - 1L);
        for (int bit = 1; bit <= 62; bit++) {
            long pow = 1L << bit;
            for (long v : new long[]{pow - 1L, pow, pow + 1L}) {
                values.add(v);
                values.add(-v);
            }
        }
        long decimal = 1L;
        for (int i = 1; i <= 18; i++) {
            decimal *= 10L;
            values.add(decimal);
            values.add(-decimal);
            values.add(decimal - 1L);
            values.add(-decimal + 1L);
        }
        Random random = new Random(SEED);
        for (int bits = 1; bits <= 63; bits++) {
            long mask = bits == 63 ? Long.MAX_VALUE : (1L << bits) - 1L;
            for (int i = 0; i < RANDOM_VALUES_PER_BIT_LENGTH; i++) {
                long v = random.nextLong() & mask;
                values.add(i % 2 == 0 ? v : -v);
            }
        }
        return values;
    }

    private static List<String> decodeInputs() {
        List<String> inputs = new ArrayList<>();
        inputs.add(null);
        inputs.add("");
        inputs.add("0");
        inputs.add("1");
        inputs.add("~");
        inputs.add("-");
        inputs.add("--");
        inputs.add("-1");
        inputs.add("--1");
        // Non-canonical but structurally valid sortable forms
        inputs.add("00");
        inputs.add("000");
        inputs.add("01");
        inputs.add("010");
        inputs.add("0~");
        inputs.add("10");
        inputs.add("11");
        inputs.add("1~");
        inputs.add("20");
        inputs.add("2AA");
        // Length-symbol sweep: valid length and off-by-one length
        String b64 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                     "_abcdefghijklmnopqrstuvwxyz~";
        char[] lengthChars = {'0', '1', '2', '9', 'A', 'F', 'G', 'Z',
                              'a', 'z', '~'};
        for (char lengthChar : lengthChars) {
            int length = b64.indexOf(lengthChar);
            inputs.add(lengthChar + repeat('1', length));
            inputs.add(lengthChar + repeat('1', length + 1));
            inputs.add("0" + lengthChar + repeat('1', length));
        }
        // Maximum structural length, decode wraps silently
        inputs.add('~' + repeat('~', 63));
        inputs.add("0~" + repeat('~', 63));
        inputs.add('~' + repeat('1', 63));
        // Characters outside the symbol table
        inputs.add("1!");
        inputs.add("2A!");
        inputs.add("2A ");
        inputs.add(" 11");
        inputs.add("11 ");
        inputs.add("2Aé");
        inputs.add("3AéB");
        inputs.add("2A€");
        inputs.add("3A😀");
        inputs.add("2A\ud83d");
        inputs.add("-é");
        return inputs;
    }

    private static void recordLongOp(Emitter emitter, String method,
                                     long value) throws IOException {
        String expected;
        try {
            expected = "ok:" + escape(callLong(method, value));
        } catch (RuntimeException e) {
            expected = "throw:" + e.getClass().getSimpleName();
        }
        emitter.line(method, String.valueOf(value), expected);
    }

    private static void recordRoundTrip(Emitter emitter, String method,
                                        String encoded, long value)
                                        throws IOException {
        emitter.line(method, escape(encoded), "ok:" + value);
    }

    private static void recordStringOp(Emitter emitter, String method,
                                       String input) throws IOException {
        String expected;
        try {
            expected = "ok:" + callString(method, input);
        } catch (RuntimeException e) {
            expected = "throw:" + e.getClass().getSimpleName();
        }
        emitter.line(method, input == null ? "\\N" : escape(input),
                     expected);
    }

    private static String callLong(String method, long value) {
        switch (method) {
            case "encodeSortable":
                return LongEncoding.encodeSortable(value);
            case "encodeSignedB64":
                return LongEncoding.encodeSignedB64(value);
            case "encodeB64":
                return LongEncoding.encodeB64(value);
            default:
                throw new IllegalArgumentException(method);
        }
    }

    private static long callString(String method, String input) {
        switch (method) {
            case "decodeSortable":
                return LongEncoding.decodeSortable(input);
            case "decodeSignedB64":
                return LongEncoding.decodeSignedB64(input);
            case "decodeB64":
                return LongEncoding.decodeB64(input);
            default:
                throw new IllegalArgumentException(method);
        }
    }

    private static String repeat(char c, int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    static String escape(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                default:
                    if (c < 0x20 || c > 0x7e) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    static String unescape(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != '\\') {
                sb.append(c);
                continue;
            }
            char next = value.charAt(++i);
            switch (next) {
                case '\\':
                    sb.append('\\');
                    break;
                case 't':
                    sb.append('\t');
                    break;
                case 'n':
                    sb.append('\n');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case 'u':
                    sb.append((char) Integer.parseInt(
                              value.substring(i + 1, i + 5), 16));
                    i += 4;
                    break;
                default:
                    throw new IllegalArgumentException(
                              "Bad escape '\\" + next + "' in: " + value);
            }
        }
        return sb.toString();
    }
}
