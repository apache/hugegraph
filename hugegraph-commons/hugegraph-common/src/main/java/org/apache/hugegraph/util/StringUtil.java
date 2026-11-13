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

package org.apache.hugegraph.util;

public final class StringUtil {

    public static Chars[] splitToCharsArray(String text, String delimiter) {
        E.checkArgument(delimiter.length() > 0,
                        "The delimiter can't be empty");
        Chars[] buffer = new Chars[text.length()];
        int count = Chars.split(text, delimiter, buffer);
        if (count == buffer.length) {
            return buffer;
        }
        Chars[] result = new Chars[count];
        System.arraycopy(buffer, 0, result, 0, count);
        return result;
    }

    public static String[] split(String text, String delimiter) {
        E.checkArgument(delimiter.length() > 0,
                        "The delimiter can't be empty");
        Chars[] buffer = new Chars[text.length()];
        int count = Chars.split(text, delimiter, buffer);
        String[] result = new String[count];
        for (int i = 0; i < count; i++) {
            result[i] = buffer[i].toString();
        }
        return result;
    }

    public static class Chars implements CharSequence {

        private final char[] chars;
        private final int start;
        private final int end;

        public Chars(char[] chars, int start, int end) {
            E.checkArgument(0 <= start && start <= chars.length,
                            "Invalid start parameter %s", start);
            E.checkArgument(start <= end && end <= chars.length,
                            "Invalid end parameter %s", end);
            this.chars = chars;
            this.start = start;
            this.end = end;
        }

        @Override
        public int length() {
            return this.end - this.start;
        }

        @Override
        public char charAt(int index) {
            return this.chars[this.start + index];
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return new Chars(this.chars, this.start + start, this.start + end);
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Chars)) {
                return false;
            }
            Chars other = (Chars) object;
            return this.toString().equals(other.toString());
        }

        @Override
        public int hashCode() {
            return this.toString().hashCode();
        }

        @Override
        public String toString() {
            return new String(this.chars, this.start, this.length());
        }

        public static Chars of(String string) {
            return new Chars(string.toCharArray(), 0, string.length());
        }

        public static Chars[] of(String... strings) {
            Chars[] results = new Chars[strings.length];
            for (int i = 0; i < strings.length; i++) {
                results[i] = Chars.of(strings[i]);
            }
            return results;
        }

        public static int split(String text, String delimiter, Chars[] buffer) {
            int count = 0;
            int from = 0;
            char[] chars = text.toCharArray();
            for (int to; (to = text.indexOf(delimiter, from)) >= 0;
                 from = to + delimiter.length()) {
                buffer[count++] = new Chars(chars, from, to);
            }
            if (from < text.length()) {
                buffer[count++] = new Chars(chars, from, text.length());
            }
            return count;
        }
    }
}
