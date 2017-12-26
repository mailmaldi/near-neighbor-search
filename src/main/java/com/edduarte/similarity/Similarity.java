package com.edduarte.similarity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.edduarte.similarity.hash.HashProvider.HashMethod;
import com.edduarte.similarity.internal.JaccardSetSimilarity;
import com.edduarte.similarity.internal.JaccardStringSimilarity;
import com.edduarte.similarity.internal.LSHSetSimilarity;
import com.edduarte.similarity.internal.LSHStringSimilarity;
import com.edduarte.similarity.internal.MinHashSetSimilarity;
import com.edduarte.similarity.internal.MinHashStringSimilarity;

/**
 * @author Eduardo Duarte (<a href="mailto:hi@edduarte.com">hi@edduarte.com</a>)
 * @version 0.0.1
 * @since 0.0.1
 */
public interface Similarity<T> {

    double calculate(T t1, T t2);


    static JaccardFactory jaccard() {
        return new JaccardFactory();
    }

    static MinHashFactory minhash() {
        return new MinHashFactory();
    }

    static LSHFactory lsh() {
        return new LSHFactory();
    }


    static double jaccardIndex(final int intersectionCount, final int unionCount) {
        return (double) intersectionCount / (double) unionCount;
    }


    static double jaccardIndex(
        final List<CharSequence> shingles1,
        final List<CharSequence> shingles2) {

        final ArrayList<Integer> r1 = new ArrayList<>();
        final ArrayList<Integer> r2 = new ArrayList<>();

        final Map<CharSequence, Integer> shingleOccurrencesMap1 = new HashMap<>();
        shingles1.forEach(s -> {
            if (shingleOccurrencesMap1.containsKey(s)) {
                final int position = shingleOccurrencesMap1.get(s).intValue();
                r1.set(position, Integer.valueOf(r1.get(position).intValue() + 1));

            } else {
                shingleOccurrencesMap1.put(s, Integer.valueOf(shingleOccurrencesMap1.size()));
                r1.add(Integer.valueOf(1));
            }
        });

        final Map<CharSequence, Integer> shingleOccurrencesMap2 = new HashMap<>();
        shingles2.forEach(s -> {
            if (shingleOccurrencesMap2.containsKey(s)) {
                final int position = shingleOccurrencesMap2.get(s).intValue();
                r2.set(position, Integer.valueOf(r2.get(position).intValue() + 1));

            } else {
                shingleOccurrencesMap2.put(s, Integer.valueOf(shingleOccurrencesMap2.size()));
                r2.add(Integer.valueOf(1));
            }
        });

        final int maxLength = Math.max(r1.size(), r2.size());

        int intersection = 0;
        int union = 0;

        for (int i = 0; i < maxLength; i++) {
            final int value1 = i < r1.size() ? r1.get(i).intValue() : 0;
            final int value2 = i < r2.size() ? r2.get(i).intValue() : 0;
            if (value1 > 0 || value2 > 0) {
                union++;

                if (value1 > 0 && value2 > 0) {
                    intersection++;
                }
            }
        }

        return jaccardIndex(intersection, union);
    }


    static double signatureIndex(final int[] signature1, final int[] signature2) {
        double similarity = 0;
        final int signatureSize = signature1.length;
        for (int i = 0; i < signatureSize; i++) {
            if (signature1[i] == signature2[i]) {
                similarity++;
            }
        }
        return similarity / signatureSize;
    }


    static boolean isCandidatePair(final int[] bands1, final int[] bands2) {
        final int bandCount = bands1.length;
        for (int b = 0; b < bandCount; b++) {
            if (bands1[b] == bands2[b]) {
                return true;
            }
        }
        return false;
    }


    static void closeExecutor(final ExecutorService exec) {
        exec.shutdown();
        try {
            exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (final InterruptedException ex) {
            final String m = "There was a problem executing the processing tasks.";
            throw new RuntimeException(m, ex);
        }
    }


    final class JaccardFactory {

        // sensible defaults for common small strings (smaller than an email)
        // or small collections (between 10 to 40 elements)
        private int k = 2;

        private ExecutorService exec;


        /**
         * Length of n-gram shingles that are used for comparison (used for
         * strings only).
         */
        public JaccardFactory withShingleLength(final int shingleLength) {
            this.k = shingleLength;
            return this;
        }


        /**
         * An executor where the kshingling tasks are spawned. If nothing is
         * provided then it launches a new executor with the cached thread pool.
         */
        public JaccardFactory withExecutor(final ExecutorService executor) {
            this.exec = executor;
            return this;
        }


        public synchronized double of(final String s1, final String s2) {
            ExecutorService e = this.exec;
            boolean usingDefaultExec = false;
            if (e == null || e.isShutdown()) {
                e = Executors.newCachedThreadPool();
                usingDefaultExec = true;
            }
            final JaccardStringSimilarity j = new JaccardStringSimilarity(e, this.k);
            final double index = j.calculate(s1, s2);
            if (usingDefaultExec) {
                closeExecutor(e);
            }
            return index;
        }


        public synchronized double of(
            final Collection<? extends Number> c1,
            final Collection<? extends Number> c2) {
            final JaccardSetSimilarity j = new JaccardSetSimilarity();
            return j.calculate(c1, c2);
        }
    }


    final class MinHashFactory {

        // sensible defaults for common small strings (smaller than an email)
        // or small collections (between 10 to 40 elements)
        private int k = 2;

        private int n = -1;

        private int sigSize = 100;

        private HashMethod h = HashMethod.Murmur3;

        private ExecutorService exec;


        /**
         * Length of n-gram shingles that are used for comparison (used for
         * strings only).
         */
        public MinHashFactory withShingleLength(final int shingleLength) {
            this.k = shingleLength;
            return this;
        }


        /**
         * Number of unique elements in both sets (used for sets only). For
         * example, if set1=[4, 5, 6, 7, 8] and set2=[7, 8, 9, 10], this value
         * should be 7. If nothing is provided, this value is determined in
         * pre-processing.
         */
        public MinHashFactory withNumberOfElements(final int elementCount) {
            this.n = elementCount;
            return this;
        }


        /**
         * The size of the generated signatures, which are compared to determine
         * similarity.
         */
        public MinHashFactory withSignatureSize(final int signatureSize) {
            this.sigSize = signatureSize;
            return this;
        }


        /**
         * The hashing algorithm used to hash shingles to signatures (used for
         * strings only).
         */
        public MinHashFactory withHashMethod(final HashMethod hashMethod) {
            this.h = hashMethod;
            return this;
        }


        /**
         * An executor where the kshingling and signature processing tasks are
         * spawned. If nothing is provided then it launches a new executor with
         * the cached thread pool.
         */
        public MinHashFactory withExecutor(final ExecutorService executor) {
            this.exec = executor;
            return this;
        }


        public synchronized double of(final String s1, final String s2) {
            ExecutorService e = this.exec;
            boolean usingDefaultExec = false;
            if (e == null || e.isShutdown()) {
                e = Executors.newCachedThreadPool();
                usingDefaultExec = true;
            }
            final MinHashStringSimilarity j = new MinHashStringSimilarity(
                    e, this.sigSize, this.h, this.k);
            final double index = j.calculate(s1, s2);
            if (usingDefaultExec) {
                closeExecutor(e);
            }
            return index;
        }


        public synchronized double of(
            final Collection<? extends Number> c1,
            final Collection<? extends Number> c2) {
            ExecutorService e = this.exec;
            boolean usingDefaultExec = false;
            if (e == null || e.isShutdown()) {
                e = Executors.newCachedThreadPool();
                usingDefaultExec = true;
            }
            int nAux = this.n;
            if (nAux < 0) {
                final Set<Number> unionSet = new HashSet<>(c1);
                unionSet.addAll(c2);
                nAux = (int) unionSet.parallelStream().distinct().count();
            }
            final MinHashSetSimilarity j = new MinHashSetSimilarity(e, nAux, this.sigSize);
            final double index = j.calculate(c1, c2);
            if (usingDefaultExec) {
                closeExecutor(e);
            }
            return index;
        }
    }


    final class LSHFactory {

        // sensible defaults for common small strings (smaller than an email)
        // or small collections (between 10 to 40 elements)
        private int k = 2;

        private int n = -1;

        private int b = 20;

        private int r = 5;

        private double s = 0.5;

        private HashMethod h = HashMethod.Murmur3;

        private ExecutorService exec;


        /**
         * Length of n-gram shingles that are used when generating signatures
         * (used for strings only).
         */
        public LSHFactory withShingleLength(final int shingleLength) {
            this.k = shingleLength;
            return this;
        }


        /**
         * Number of unique elements in both sets (used for sets only). For
         * example, if set1=[4, 5, 6, 7, 8] and set2=[7, 8, 9, 10], this value
         * should be 7. If nothing is provided, this value is determined in
         * pre-processing.
         */
        public LSHFactory withNumberOfElements(final int elementCount) {
            this.n = elementCount;
            return this;
        }


        /**
         * The number of bands where the minhash signatures will be structured.
         */
        public LSHFactory withNumberOfBands(final int bandCount) {
            this.b = bandCount;
            return this;
        }


        /**
         * The number of rows where the minhash signatures will be structured.
         */
        public LSHFactory withNumberOfRows(final int rowCount) {
            this.r = rowCount;
            return this;
        }


        /**
         * A threshold S that balances the number of false positives and false
         * negatives.
         */
        public LSHFactory withThreshold(final int threshold) {
            this.s = threshold;
            return this;
        }


        /**
         * The hashing algorithm used to hash shingles to signatures (used for
         * strings only).
         */
        public LSHFactory withHashMethod(final HashMethod hashMethod) {
            this.h = hashMethod;
            return this;
        }


        /**
         * An executor where the kshingling and signature processing tasks are
         * spawned. If nothing is provided then it launches a new executor with
         * the cached thread pool.
         */
        public LSHFactory withExecutor(final ExecutorService executor) {
            this.exec = executor;
            return this;
        }


        public synchronized double of(final String s1, final String s2) {
            ExecutorService e = this.exec;
            boolean usingDefaultExec = false;
            if (e == null || e.isShutdown()) {
                e = Executors.newCachedThreadPool();
                usingDefaultExec = true;
            }
            final LSHStringSimilarity j = new LSHStringSimilarity(e, this.b, this.r, this.s, this.h, this.k);
            final double index = j.calculate(s1, s2);
            if (usingDefaultExec) {
                closeExecutor(e);
            }
            return index;
        }


        public synchronized double of(
            final Collection<? extends Number> c1,
            final Collection<? extends Number> c2) {
            ExecutorService e = this.exec;
            boolean usingDefaultExec = false;
            if (e == null || e.isShutdown()) {
                e = Executors.newCachedThreadPool();
                usingDefaultExec = true;
            }
            int nAux = this.n;
            if (nAux < 0) {
                final Set<Number> unionSet = new HashSet<>(c1);
                unionSet.addAll(c2);
                nAux = (int) unionSet.parallelStream().distinct().count();
            }
            final LSHSetSimilarity j = new LSHSetSimilarity(e, nAux, this.b, this.r, this.s);
            final double index = j.calculate(c1, c2);
            if (usingDefaultExec) {
                closeExecutor(e);
            }
            return index;
        }
    }
}
