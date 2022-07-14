package com.techteam.beam.main;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.Objects;

public class JoinExamplesMain {
    public static void main(String[] args) {
        runBigQueryJoin();
    }

    private static void runBigQueryJoin() {
        String products = "/data/section1/products.csv";
        String productTypes = "/data/section1/product_types.csv";
        Pipeline pipeline = Pipeline.create();

        Schema productSchema = Schema.of(
                Schema.Field.of("ProductId", Schema.FieldType.INT32),
                Schema.Field.of("ProductName", Schema.FieldType.STRING),
                Schema.Field.of("ProductTypeId", Schema.FieldType.INT32),
                Schema.Field.of("Price", Schema.FieldType.INT32)
        );Schema productTypeSchema = Schema.of(Schema.Field.of("ProductTypeId", Schema.FieldType.INT32),
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

        //Inner Join
//        PCollection<Row> joinCollection = productTypePCollection
//                .apply("Create Join", Join.<Row, Row>innerJoin(productPCollection)
//                        .using("ProductTypeId")
////                        .on(Join.FieldsEqual.left("ProductTypeId")
////                                .right("ProductId")
////                        )
//                );

        //Left Join
//        PCollection<Row> joinCollection = productTypePCollection
//                .apply("Create Join", Join.<Row, Row>leftOuterJoin(productPCollection)
//                        .using("ProductTypeId")
////                        .on(Join.FieldsEqual.left("ProductTypeId")
////                                .right("ProductTypeId")
////                        )
//                );

        //Right Join
        PCollection<Row> joinCollection = productTypePCollection
                .apply("Create Join", Join.<Row, Row>rightOuterJoin(productPCollection)
                        .using("ProductTypeId")
//                        .on(Join.FieldsEqual.left("ProductTypeId")
//                                .right("ProductTypeId")
//                        )
                );

        //Full Join
//        PCollection<Row> joinCollection = productTypePCollection
//                .apply("Create Join", Join.<Row, Row>fullOuterJoin(productPCollection)
//                        //.using("ProductTypeId")
//                        .on(Join.FieldsEqual.left("ProductTypeId")
//                                .right("ProductTypeId")
//                        )
//                );

        PCollection<String> resultCollection = joinCollection
                .apply(ParDo.of(new DoFn<Row, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Row rows = c.element();
                        assert rows != null;
                        Row productTypeRow = rows.getRow(0);
                        Row productRow = rows.getRow(1);

                        System.out.println("rows ---> " + rows);
                        c.output(String.join(",", checkValue(productRow, 0),
                                checkValue(productRow, 1),
                                checkValue(productRow, 3),
                                checkValue(productTypeRow, 1)
                        ));
                    }
                    private CharSequence checkValue(Row row, int index) {
                        return Objects.isNull(row) ? null : row.getValue(index) + "";
                    }
                }));

        resultCollection
                .apply(TextIO
                .write().withoutSharding()
                .to("result/join_products")
                .withHeader("ProductId, ProductName, Price, ProductType")
                .withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
}
