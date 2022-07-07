package com.techteam.beam.main;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

public class GroupByKeyExamplesMain {
    public static void main(String[] args) {
        runGroupByKeyBigQuery();
    }

    private static void runGroupByKeyBigQuery() {
        String products = "/data/section1/products.csv";
        String productTypes = "/data/section1/product_types.csv";
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<Integer, String>> productCollection =
                pipeline.apply(TextIO.read().from(products))
                        .apply("FilterHeader", Filter.by(line ->
                                !line.isEmpty() && !line.contains("ProductId, ProductName, ProductTypeId, Price")))
                                .apply(ParDo.of(new DoFn<String, KV<Integer, String>>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        String row = c.element();
                                        assert row != null;
                                        String[] splits = row.split(",");

                                        c.output(KV.of(Integer.valueOf(splits[0]), String.join(",",
                                                splits[0], splits[1], splits[2], splits[3])));
                                    }
                                }));

        PCollection<KV<Integer, String>> productTypeCollection =
                pipeline.apply(TextIO.read().from(productTypes))
                        .apply("FilterHeader", Filter.by(line ->
                                !line.isEmpty() && !line.contains("ProductTypeId, ProductType")))
                        .apply(ParDo.of(new DoFn<String, KV<Integer, String>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                String row = c.element();
                                assert row != null;
                                String[] splits = row.split(",");
                                c.output(KV.of(Integer.valueOf(splits[0]), String.join(",",
                                        splits[0], splits[1])));
                            }
                        }));

        TupleTag<String> productTag = new TupleTag<>();
        TupleTag<String> productTypeTag = new TupleTag<>();

        PCollection<KV<Integer, CoGbkResult>> result = KeyedPCollectionTuple.of(productTag, productCollection)
                        .and(productTypeTag, productTypeCollection).apply(CoGroupByKey.create());

        result.apply("Preview grouped data",
                MapElements.into(TypeDescriptors.strings()).via(
                        x -> { System.out.println("x ----> " + x); return ""; })
        );

        pipeline.run().waitUntilFinish();
    }
}
