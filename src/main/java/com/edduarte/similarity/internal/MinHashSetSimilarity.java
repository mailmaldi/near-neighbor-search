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

package com.edduarte.similarity.internal;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.edduarte.similarity.SetSimilarity;
import com.edduarte.similarity.Similarity;
import com.edduarte.similarity.converter.Set2SignatureConverter;

/**
 * @author Eduardo Duarte (<a href="mailto:hi@edduarte.com">hi@edduarte.com</a>)
 * @version 0.0.1
 * @since 0.0.1
 */
public class MinHashSetSimilarity implements SetSimilarity {

    private final Set2SignatureConverter p;

    private final ExecutorService exec;


    /**
     * Instantiates a Similarity class for number sets using the MinHashing
     * algorithm.
     *
     * @param exec    the executor that will receive the concurrent shingle
     *                processing tasks
     * @param n       the total number of unique elements in both sets
     * @param sigSize the length of the signature array to be generated
     */
    public MinHashSetSimilarity(final ExecutorService exec, final int n, final int sigSize) {
        this.exec = exec;
        this.p = new Set2SignatureConverter(n, sigSize);
    }


    @Override
    public double calculate(
        final Collection<? extends Number> c1,
        final Collection<? extends Number> c2) {
        final Future<int[]> signatureFuture1 = this.exec.submit(this.p.apply(c1));
        final Future<int[]> signatureFuture2 = this.exec.submit(this.p.apply(c2));

        try {
            final int[] signature1 = signatureFuture1.get();
            final int[] signature2 = signatureFuture2.get();

            return Similarity.signatureIndex(signature1, signature2);

        } catch (ExecutionException | InterruptedException ex) {
            final String m = "There was a problem processing set signatures.";
            throw new RuntimeException(m, ex);
        }
    }
}
