package com.techteam.beam;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

public class TestEntirePipeLine {
    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testCompositeTransforms() {
        final Integer[] inputs = {1, 2, 3, 4, 5};
        PCollection<Integer> input = pipeline.apply(Create.of(Arrays.asList(inputs)));

        KV[] kvs = {KV.of("1", 1L),
                KV.of("2", 1L),
                KV.of("3", 1L),
                KV.of("4", 1L),
                KV.of("5", 1L)};
        PCollection<KV<String, Long>> outputCountNumbers = input
                        .apply(ToString.elements())
                        .apply(new CountNumbers());

        Integer expected = 15;
        PCollection<Integer> outputSumNumbers = input.apply("Create sum numbers output", new TestCompositeTransforms.SumNumbers());

        PAssert.that(outputCountNumbers).containsInAnyOrder(kvs);
        PAssert.that(outputSumNumbers).containsInAnyOrder(expected);

        pipeline.run();
    }

    static class CountNumbers extends PTransform<PCollection<String>,
            PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
            return lines.apply(Count.perElement());
        }
    }

    static class SumNumbers extends PTransform<PCollection<Integer>, PCollection<Integer>> {
        @Override
        public PCollection<Integer> expand(PCollection<Integer> input) {
            return input.apply(Combine.globally(new TestCompositeTransforms.SumNumbers.SumInts()));
        }

        static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {

            @Override
            public Integer apply(Iterable<Integer> input) {
                int sum = 0;
                for (Integer number : input) {
                    sum += number;
                }
                return sum;
            }
        }
    }
}
