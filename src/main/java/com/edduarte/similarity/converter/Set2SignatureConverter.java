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

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Processor class to retrieve shingles of length k.
 *
 * @author Eduardo Duarte (<a href="mailto:hi@edduarte.com">hi@edduarte.com</a>)
 * @version 0.0.1
 * @since 0.0.1
 */
public class Set2SignatureConverter
        implements Function<Collection<? extends Number>, Callable<int[]>>, Serializable
{

    private static final int LARGE_PRIME = 433494437;

    private static final long serialVersionUID = 5407020782327532877L;
    /**
     * Random coefficient "a" for the random hash functions
     */
    private final int[] a;


    /**
     * Random coefficient "b" for the random hash functions
     */
    private final int[] b;


    /**
     * Expected maximum size of the sets to test. This
     * will be the size of the hash signatures.
     */
    private final int n;


    /**
     * Size of min hashes that will be stored and compared to find the
     * similarity index
     */
    private final int sigSize;


    /**
     * Initializes hashing functions to compute MinHash signatures for sets that
     * could have a maximum count calculate 'n' elements with a given signature size.
     */
    public Set2SignatureConverter(final int n, final int sigSize) {
        this.n = n;
        this.sigSize = sigSize;
        final SecureRandom r = new SecureRandom();
        this.a = new int[this.sigSize];
        this.b = new int[this.sigSize];
        for (int i = 0; i < this.sigSize; i++) {
            this.a[i] = 1 + r.nextInt(this.n - 1);
            this.b[i] = r.nextInt(this.n);
        }
    }


    @Override
    public Callable<int[]> apply(final Collection<? extends Number> set) {
        return new HashCallable(this.n, this.sigSize, this.a, this.b, set);
    }

    public int[] compute(final Collection<? extends Number> set)
    {
        final int[] signature = new int[this.sigSize];
        for (int i = 0; i < this.sigSize; i++)
        {
            signature[i] = Integer.MAX_VALUE;
        }
            //IntStream.range(0, this.sigSize).map(i -> Integer.MAX_VALUE).toArray();

//        final List<? extends Number> list = new ArrayList<>(set);
//        list.sort(Comparator.comparingLong(Number::longValue));

        for (final Number x : set) {
            for (int i = 0; i < this.sigSize; i++) {
                signature[i] = Math.min(signature[i], universalHashing(i, x));
            }
        }

        return signature;
    }

    private int universalHashing(final int i, final Number x) {
        return (int) ((this.a[i] * x.longValue() + this.b[i]) % LARGE_PRIME); // TODO removed %n
    }


    private static class HashCallable implements Callable<int[]> {

        private final int n;

        private final int sigSize;

        private final int[] a;

        private final int[] b;

        private final Collection<? extends Number> set;


        private HashCallable(
            final int n, final int sigSize,
            final int[] a, final int[] b,
            final Collection<? extends Number> set) {
            this.n = n;
            this.sigSize = sigSize;
            this.a = a;
            this.b = b;
            this.set = set;
        }


        @Override
        public int[] call()
        {
            final int[] signature = IntStream.range(0, this.sigSize).map(i -> Integer.MAX_VALUE).toArray();

            final List<? extends Number> list = new ArrayList<>(this.set);
            list.sort(Comparator.comparingLong(Number::longValue));

            for (final Number x : list) {
                for (int i = 0; i < this.sigSize; i++) {
                    signature[i] = Math.min(signature[i], universalHashing(i, x));
                }
            }

            return signature;
        }


        private int universalHashing(final int i, final Number x) {
            return (int) ((this.a[i] * x.longValue() + this.b[i]) % LARGE_PRIME); // TODO removed %n
        }
    }
}
