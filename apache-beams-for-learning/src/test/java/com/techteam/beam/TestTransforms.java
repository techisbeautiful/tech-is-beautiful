package com.techteam.beam;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import java.util.Arrays;

public class TestTransforms {
    @Rule public TestPipeline p = TestPipeline.create();

    @Test
    public void testTranforms() {
        final Integer[] inputs = {1, 2, 3, 4, 5};
        final String[] expected = {"1", "2", "3", "4", "5"};

        PCollection<Integer> input = p.apply(Create.of(Arrays.asList(inputs)));
        PCollection<String> output = input.apply(ToString.elements());

        PAssert.that(output).containsInAnyOrder(expected);

        p.run();
    }
}
