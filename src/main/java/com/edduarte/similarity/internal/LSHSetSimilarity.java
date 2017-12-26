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
import com.edduarte.similarity.converter.Signature2BandsConverter;

/**
 * @author Eduardo Duarte (<a href="mailto:hi@edduarte.com">hi@edduarte.com</a>)
 * @version 0.0.1
 * @since 0.0.1
 */
public class LSHSetSimilarity implements SetSimilarity {

    private final JaccardSetSimilarity jaccard;

    private final Set2SignatureConverter sigp;

    private final Signature2BandsConverter bandp;

    private final ExecutorService exec;


    /**
     * Instantiates a Similarity class for number sets using the LSH algorithm.
     *
     * @param exec the executor that will receive the concurrent signature and
     *             band processing tasks
     * @param n    the total number of unique elements in both sets
     * @param b    the number of bands
     * @param r    the number of rows
     * @param s    the threshold (value between 0.0 and 1.0) that balances the
     *             trade-off between the number of false positives and false
     *             negatives. A sensible threshold is 0.5, so we have a equal
     *             number of false positives and false negatives.
     */
    public LSHSetSimilarity(final ExecutorService exec, final int n, final int b, final int r, final double s) {
        // signature size is determined by a threshold S
        this.exec = exec;
        final int R = (int) Math.ceil(Math.log(1.0 / b) / Math.log(s)) + 1;
        final int sigSize = R * b;
        this.jaccard = new JaccardSetSimilarity();
        this.sigp = new Set2SignatureConverter(n, sigSize);
        this.bandp = new Signature2BandsConverter(b, r);
    }


    @Override
    public double calculate(
        final Collection<? extends Number> c1,
        final Collection<? extends Number> c2) {
        return isCandidatePair(c1, c2) ?
            this.jaccard.calculate(c1, c2) : 0;
    }


    private boolean isCandidatePair(
        final Collection<? extends Number> c1,
        final Collection<? extends Number> c2) {
        try {
            final Future<int[]> signatureFuture1 = this.exec.submit(this.sigp.apply(c1));
            final Future<int[]> signatureFuture2 = this.exec.submit(this.sigp.apply(c2));

            final int[] signature1 = signatureFuture1.get();
            final int[] signature2 = signatureFuture2.get();

            final Future<int[]> bandsFuture1 = this.exec.submit(this.bandp.apply(signature1));
            final Future<int[]> bandsFuture2 = this.exec.submit(this.bandp.apply(signature2));

            final int[] bands1 = bandsFuture1.get();
            final int[] bands2 = bandsFuture2.get();

            return Similarity.isCandidatePair(bands1, bands2);

        } catch (ExecutionException | InterruptedException ex) {
            final String m = "There was a problem processing set signatures.";
            throw new RuntimeException(m, ex);
        }
    }
}
