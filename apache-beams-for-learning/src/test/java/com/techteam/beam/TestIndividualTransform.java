package com.techteam.beam;

import org.apache.arrow.flatbuf.Int;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

public class TestIndividualTransform {
    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testIndividualTranforms() {
        final Integer[] inputs = {1, 2, 3, 4, 5};
        PCollection<Integer> input = pipeline.apply(Create.of(Arrays.asList(inputs)));
        final String[] expected = {"1", "2", "3", "4", "5"};

        /*
        PCollection<String> output = input.apply("Create Output", ParDo.of(new DoFn<Integer, String>() {
            //anonymous transform
            @ProcessElement
            public void processElement(ProcessContext c, @Element Integer element) {
                c.output(element.toString());
            }
        })); */

        PCollection<String> output = input.apply("Create Output", ParDo.of(new TestDoFn()));

        PAssert.that(output).containsInAnyOrder(expected);

        pipeline.run();
    }

    private static class TestDoFn extends DoFn<Integer, String> {

        @ProcessElement
        public void processElement(ProcessContext c, @Element Integer number) {
            c.output(number.toString());
        }
    }
}
