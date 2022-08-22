package com.techteam.beam;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

public class TestCompositeTransforms {

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testCompositeTransforms() {
        final Integer[] inputs = {1, 2, 3, 4, 5};
        PCollection<Integer> input = pipeline.apply(Create.of(Arrays.asList(inputs)));
        Integer expected = 15;
        PCollection<Integer> output = input.apply("Create output", new SumNumbers());
        PAssert.that(output).containsInAnyOrder(expected);

        pipeline.run();
    }

    static class SumNumbers extends PTransform<PCollection<Integer>, PCollection<Integer>> {
        @Override
        public PCollection<Integer> expand(PCollection<Integer> input) {
            return input.apply(Combine.globally(new SumInts()));
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
