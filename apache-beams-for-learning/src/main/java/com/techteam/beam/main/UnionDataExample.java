package com.techteam.beam.main;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class UnionDataExample {

    public static void main(String[] args) {
        runQueryUnion();
    }

    private static void runQueryUnion() {
        String products1 = "/data/section1/products-20220821.csv";
        String products2 = "/data/section1/products-20220822.csv";
        Pipeline pipeline = Pipeline.create();

        PCollection<String> pc1 = pipeline
                .apply(TextIO.read().from(products1))
                .apply("FilterHeader", Filter.by(line -> !line.isEmpty()
                        && !line.contains("ProductId, ProductName, ProductTypeId, Price")))
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String row = c.element();
                        String[] splits = row.split(",");
                        c.output(String.join(",",
                                splits[0],
                                splits[1],
                                splits[2],
                                splits[3],
                                products1
                        ));
                    }
                }));

        PCollection<String> pc2 = pipeline
                .apply(TextIO.read().from(products2))
                .apply("FilterHeader", Filter.by(line -> !line.isEmpty()
                        && !line.contains("ProductId, ProductName, ProductTypeId, Price")))
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String row = c.element();
                        assert row != null;
                        String[] splits = row.split(",");
                        c.output(String.join(",",
                                splits[0],
                                splits[1],
                                splits[2],
                                splits[3],
                                products2
                        ));
                    }
                }));

    }
}
