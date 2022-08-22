package com.techteam.beam.main;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Iterator;
import java.util.Objects;

public class GroupByProductType {
    public static void main(String[] args) {
        runGroupQuery();
    }

    private static void runGroupQuery() {
        String products = "/data/section1/products.csv";
        String productTypes = "/data/section1/product_types.csv";
        Pipeline pipeline = Pipeline.create();

        Schema productSchema = Schema.of(
                Schema.Field.of("ProductId", Schema.FieldType.INT32),
                Schema.Field.of("ProductName", Schema.FieldType.STRING),
                Schema.Field.of("ProductTypeId", Schema.FieldType.INT32),
                Schema.Field.of("Price", Schema.FieldType.INT32)
        );
        Schema productTypeSchema = Schema.of(Schema.Field.of("ProductTypeId", Schema.FieldType.INT32),
                Schema.Field.of("ProductType", Schema.FieldType.STRING)
        );

        PCollection<Row> productPCollection = pipeline
                .apply(TextIO.read().from(products))
                .apply("FilterHeader", Filter.by(line -> !line.isEmpty()
                        && !line.contains("ProductId, ProductName, ProductTypeId, Price")))
                .apply(ParDo.of(new DoFn<String, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String row = c.element();
                        assert row != null;
                        String[] splits = row.split(",");
                        c.output(Row.withSchema(productSchema)
                                .addValue(Integer.valueOf(splits[0].trim()))
                                .addValue(splits[1].trim())
                                .addValue(Integer.valueOf(splits[2].trim()))
                                .addValue(Integer.valueOf(splits[3].trim()))
                                .build());
                    }
                })).setRowSchema(productSchema)
                ;

        PCollection<Row> productTypePCollection = pipeline
                .apply(TextIO.read().from(productTypes))
                .apply("FilterHeader", Filter.by(line -> !line.isEmpty()
                        && !line.contains("ProductTypeId, ProductType")))
                .apply(ParDo.of(new DoFn<String, Row>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String row = c.element();
                        assert row != null;
                        String[] splits = row.split(",");
                        c.output(Row.withSchema(productTypeSchema)
                                .addValue(Integer.valueOf(splits[0].trim()))
                                .addValue(splits[1].trim())
                                .build());
                    }
                })).setRowSchema(productTypeSchema)
                ;
        PCollection<KV<Integer, String>> productType = productTypePCollection
                .apply(ParDo.of(new DoFn<Row, KV<Integer, String>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                Row rows = c.element();
                                assert rows != null;
                                c.output(KV.of(rows.getValue(0),
                                        String.join(",", checkValue(rows, 0), checkValue(rows, 1))));
                            }
                        })
                );

        PCollection<KV<Integer, String>> joinCollection = productTypePCollection
                .apply("Create Join", Join.<Row, Row>leftOuterJoin(productPCollection)
                        .using("ProductTypeId")
                ).apply(ParDo.of(new DoFn<Row, KV<Integer, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Row rows = c.element();
                        assert rows != null;
                        Row productTypeRow = rows.getRow(0);
                        Row productRow = rows.getRow(1);

                        c.output(KV.of(Integer.valueOf(checkValue(productTypeRow, 0) + ""),
                                String.join(",",
                                        checkValue(productRow, 0),
                                        checkValue(productRow, 1),
                                        checkValue(productRow, 3),
                                        checkValue(productTypeRow, 1))));
                    }
                }));

        TupleTag<String> productTag = new TupleTag<>();
        TupleTag<String> productTypeTag = new TupleTag<>();

        PCollection<KV<Integer, CoGbkResult>> groupResult = KeyedPCollectionTuple.of(productTag, joinCollection)
                .and(productTypeTag, productType).apply(CoGroupByKey.create());

        PCollection<String> resultCollection = groupResult
                .apply(ParDo.of(new DoFn<KV<Integer, CoGbkResult>, String>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        KV<Integer, CoGbkResult> e = c.element();
                        assert e != null;
                        Iterable<String> productsIter = Objects.requireNonNull(e.getValue()).getAll(productTag);
                        Iterable<String> producrTypesIter = e.getValue().getAll(productTypeTag);

                        StringBuilder group = new StringBuilder();
                        StringBuilder row = new StringBuilder();
                        for (String localInputName : producrTypesIter) {
                            group.append(String.join(",", localInputName));
                        }

                        Iterator<String> iterator = productsIter.iterator();
                        double totalPrice = 0;
                        while(iterator.hasNext()) {
                            String localInputName = iterator.next();
                            String[] products = localInputName.split(",");
                            if (products[2] == null || products[2].equals("null")) products[2] = "0";
                            totalPrice += Double.parseDouble(products[2]);
                        }
                        row.append(String.join(",", group, String.valueOf(totalPrice)));

                        if (row.length() > 0) {
                            c.output(row.toString());
                        }
                    }
                }));

        resultCollection
                .apply(TextIO
                        .write().withoutSharding()
                        .to("result/group_products")
                        .withHeader("ProductTypeId, ProductTypeName, Sum")
                        .withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }

    private static CharSequence checkValue(Row row, int index) {
        return Objects.isNull(row) ? null : row.getValue(index) + "";
    }
}
