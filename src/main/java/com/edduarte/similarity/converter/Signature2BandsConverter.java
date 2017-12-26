/*
 * Copyright 2017 Eduardo Duarte
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.edduarte.similarity.converter;


import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * Processor class to retrieve shingles of length k.
 *
 * @author Eduardo Duarte (<a href="mailto:hi@edduarte.com">hi@edduarte.com</a>)
 * @version 0.0.1
 * @since 0.0.1
 */
public class Signature2BandsConverter
        implements Function<int[], Callable<int[]>> {

    private final int b;

    private final int r;


    public Signature2BandsConverter(final int b, final int r) {
        this.b = b;
        this.r = r;
    }


    @Override
    public Callable<int[]> apply(final int[] sig) {
        return new BandsCallable(sig, this.b, this.r);
    }


    private static class BandsCallable implements Callable<int[]> {

        private static final int LARGE_PRIME = 433494437;

        private final int[] sig;

        private final int b;

        private final int r;


        private BandsCallable(final int[] sig, final int b, final int r) {
            this.sig = sig;
            this.b = b;
            this.r = r;
        }


        @Override
        public int[] call() throws Exception {
            final int sigSize = this.sig.length;
            final int[] res = new int[this.b];
            final int buckets = sigSize / this.b;

            for (int i = 0; i < sigSize; i++) {
                final int band = Math.min(i / buckets, this.b - 1);
                res[band] = (int) (res[band] + (long) this.sig[i] * LARGE_PRIME); // TODO removed % r
            }

            return res;
        }
    }
}
