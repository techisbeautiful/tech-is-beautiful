package com.techteam.beam.main;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.Iterator;
import java.util.Objects;

public class MergeDataExample {
    public static void main(String[] args) {
        runQueryMerge();
    }

    private static void runQueryMerge() {
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

        PCollection<String> merged = PCollectionList.of(pc1).and(pc2)
                .apply(Flatten.pCollections());


        PCollection<KV<Integer, String>> keyValues = merged
                .apply(ParDo.of(new DoFn<String, KV<Integer, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String row = c.element();
                        assert row != null;
                        String[] splits = row.split(",");
                        c.output(KV.of(Integer.valueOf(splits[0]), String.join(",",
                                splits[0],
                                splits[1],
                                splits[2],
                                splits[3],
                                splits[4]
                        )));
                    }
                }));

        PCollection<KV<Integer, Iterable<String>>> pcs = keyValues.apply(GroupByKey.create());
        PCollection<String> results = pcs
                .apply(ParDo.of(new DoFn<KV<Integer, Iterable<String>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Integer key = Objects.requireNonNull(c.element()).getKey();
                        Iterable<String> strings = Objects.requireNonNull(c.element()).getValue();
                        assert strings != null;
                        Iterator<String> productsIter = strings.iterator();

                        KV<Integer, String> kv = KV.of(key, "");
                        while(productsIter.hasNext()) {
                            String localInputName = productsIter.next();
                            String[] splits = localInputName.split(",");
                            String keyString = splits[4];
                            if (keyString.equals(products2)) {
                                kv = KV.of(key, localInputName);
                            }
                        }
                        String[] splits = Objects.requireNonNull(kv.getValue()).split(",");
                        c.output(String.join(",",
                                splits[0],
                                splits[1],
                                splits[2],
                                splits[3]
                        ));
                    }
                }));

        results
                .apply(TextIO
                        .write().withoutSharding()
                        .to("result/merged_products")
                        .withHeader("ProductId, ProductName, ProductTypeId, Price")
                        .withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
}
