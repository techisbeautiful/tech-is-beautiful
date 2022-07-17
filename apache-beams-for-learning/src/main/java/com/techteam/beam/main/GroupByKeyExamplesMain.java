package com.techteam.beam.main;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

import java.util.Iterator;

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

                                        //key: ProductTypeId
                                        //values: ProductId, ProductName, ProductTypeId, Price
                                        c.output(KV.of(Integer.valueOf(splits[2].trim()), String.join(",",
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

        PCollection<KV<Integer, CoGbkResult>> groupResult = KeyedPCollectionTuple.of(productTag, productCollection)
                        .and(productTypeTag, productTypeCollection).apply(CoGroupByKey.create());

        PCollection<String> resultInnerCollection = groupResult
                .apply(ParDo.of(new DoFn<KV<Integer, CoGbkResult>, String>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        KV<Integer, CoGbkResult> e = c.element();
                        Iterable<String> productsIter = e.getValue().getAll(productTag);
                        Iterable<String> producrTypesIter = e.getValue().getAll(productTypeTag);

                        StringBuilder group = new StringBuilder();
                        StringBuilder row = new StringBuilder();
                        for (String localInputName : producrTypesIter) {
                            group.append(String.join(",", localInputName));
                        }

                        Iterator<String> iterator = productsIter.iterator();
                        while(iterator.hasNext()) {
                            String localInputName = iterator.next();
                            String[] products = localInputName.split(",");
                            String[] productTypes = group.toString().split(",");

                            if (productTypes.length > 1) {
                                row.append(String.join(",", products[0], products[1], products[3], productTypes[1]));
                                if (iterator.hasNext()) {
                                    row.append("\n");
                                }
                            }

                        }
                        if (row.length() > 0) {
                            c.output(row.toString());
                        }
                    }
                }));

        PCollection<String> resultLeftCollection = groupResult
                .apply(ParDo.of(new DoFn<KV<Integer, CoGbkResult>, String>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        KV<Integer, CoGbkResult> e = c.element();
                        Iterable<String> productsIter = e.getValue().getAll(productTag);
                        Iterable<String> producrTypesIter = e.getValue().getAll(productTypeTag);

                        StringBuilder group = new StringBuilder();
                        StringBuilder row = new StringBuilder();
                        for (String localInputName : producrTypesIter) {
                            group.append(String.join(",", localInputName));
                        }

                        Iterator<String> iterator = productsIter.iterator();
                        String[] productTypes = group.toString().split(",");
                        while(iterator.hasNext()) {
                            String localInputName = iterator.next();
                            String[] products = localInputName.split(",");
                            if (productTypes.length > 1) {
                                row.append(String.join(",", products[0], products[1], products[3], productTypes[1]));
                                if (iterator.hasNext()) {
                                    row.append("\n");
                                }
                            }
                        }

                        if (row.length() > 0) {
                            c.output(row.toString());
                        } else if (group.length() > 0 && row.length() == 0) {
                            c.output(String.join(",", null, null, null, productTypes[1]));
                        }
                    }
                }));

        PCollection<String> resultRightCollection = groupResult
                .apply(ParDo.of(new DoFn<KV<Integer, CoGbkResult>, String>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        KV<Integer, CoGbkResult> e = c.element();
                        Iterable<String> productsIter = e.getValue().getAll(productTag);
                        Iterable<String> producrTypesIter = e.getValue().getAll(productTypeTag);

                        StringBuilder group = new StringBuilder();
                        StringBuilder row = new StringBuilder();
                        for (String localInputName : producrTypesIter) {
                            group.append(String.join(",", localInputName));
                        }

                        Iterator<String> iterator = productsIter.iterator();
                        String[] productTypes = group.toString().split(",");
                        while(iterator.hasNext()) {
                            String localInputName = iterator.next();
                            String[] products = localInputName.split(",");
                            if (productTypes.length > 1) {
                                row.append(String.join(",", products[0], products[1], products[3], productTypes[1]));
                            } else {
                                row.append(String.join(",", products[0], products[1], products[3], null));
                            }
                            if (iterator.hasNext()) {
                                row.append("\n");
                            }
                        }

                        if (row.length() > 0) {
                            c.output(row.toString());
                        }
                    }
                }));

        PCollection<String> resultFullCollection = groupResult
                .apply(ParDo.of(new DoFn<KV<Integer, CoGbkResult>, String>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        KV<Integer, CoGbkResult> e = c.element();
                        Iterable<String> productsIter = e.getValue().getAll(productTag);
                        Iterable<String> producrTypesIter = e.getValue().getAll(productTypeTag);

                        StringBuilder group = new StringBuilder();
                        StringBuilder row = new StringBuilder();
                        for (String localInputName : producrTypesIter) {
                            group.append(String.join(",", localInputName));
                        }

                        Iterator<String> iterator = productsIter.iterator();
                        String[] productTypes = group.toString().split(",");
                        while(iterator.hasNext()) {
                            String localInputName = iterator.next();
                            String[] products = localInputName.split(",");
                            if (productTypes.length > 1) {
                                row.append(String.join(",", products[0], products[1], products[3], productTypes[1]));
                            } else {
                                row.append(String.join(",", products[0], products[1], products[3], null));
                            }
                            if (iterator.hasNext()) {
                                row.append("\n");
                            }
                        }

                        if (row.length() > 0) {
                            c.output(row.toString());
                        } else if (group.length() > 0 && row.length() == 0) {
                            c.output(String.join(",", null, null, null, productTypes[1]));
                        }
                    }
                }));

        resultInnerCollection
                .apply(TextIO
                        .write().withoutSharding()
                        .to("result/group_inner_join_products")
                        .withHeader("ProductId, ProductName, Price, ProductType")
                        .withSuffix(".csv"));

        resultLeftCollection
                .apply(TextIO
                        .write().withoutSharding()
                        .to("result/group_left_join_products")
                        .withHeader("ProductId, ProductName, Price, ProductType")
                        .withSuffix(".csv"));

        resultRightCollection
                .apply(TextIO
                        .write().withoutSharding()
                        .to("result/group_right_join_products")
                        .withHeader("ProductId, ProductName, Price, ProductType")
                        .withSuffix(".csv"));

        resultFullCollection
                .apply(TextIO
                        .write().withoutSharding()
                        .to("result/group_full_join_products")
                        .withHeader("ProductId, ProductName, Price, ProductType")
                        .withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
}
