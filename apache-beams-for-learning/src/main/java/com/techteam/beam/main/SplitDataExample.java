package com.techteam.beam.main;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class SplitDataExample {
    public static void main(String[] args) {
        runQuerySplit();
    }

    private static void runQuerySplit() {
        String products = "/data/section1/products-20220822.csv";
        Pipeline pipeline = Pipeline.create();

        PCollection<String> pc = pipeline
                .apply(TextIO.read().from(products))
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
                                        splits[3]
                                ));
                    }
                }));


        PCollectionList<String> results = pc.apply(Partition.of(2, (Partition.PartitionFn<String>) (elem, numPartitions) -> {
            String[] splits = elem.split(",");
            double price = Double.parseDouble(splits[3]);
            return price > 550 ? 0 : 1;
        }));

        for (int i = 0; i < 2; i++) {
            PCollection<String> partition = results.get(i);
            partition
                .apply(TextIO
                        .write().withoutSharding()
                        .to("result/partition_products-" + (i+1))
                        .withHeader("ProductId, ProductName, ProductTypeId, Price")
                        .withSuffix(".csv"));
        }
        pipeline.run().waitUntilFinish();
    }
}
