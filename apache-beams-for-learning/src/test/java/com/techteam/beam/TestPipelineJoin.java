package com.techteam.beam;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.Objects;

public class TestPipelineJoin implements Serializable {
    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    @Test
//    @Category(NeedsRunner.class)
    public void myPipelineTest() throws Exception {
        Pipeline pipeline = Pipeline.create();

//        ImmutableList<TableRow> input = ImmutableList.of(
//                new TableRow()
//                        .set("UserID", "22222")
//                        .set("Price", 20.1),
//                new TableRow()
//                        .set("UserID", "22222")
//                        .set("Price", 20.0));
//
//        PCollection<KV<String, Double>> out = p
//                .apply("Create input", Create.of(input))
//                .apply("Parse pipeline",
//                        ParDo.of(new DoFn<TableRow, KV<String, Double>>() {
//                            @ProcessElement
//                            public void processElement(ProcessContext c) {
//
//                                TableRow element = c.element();
//                                // Check if the values returned from BigQuery are null, as all columns in BigQuery are nullable
//                                String key = element.get("UserID") == null ? null : (String) element.get("UserID");
//                                Double price = element.get("Price") == null ? null : (Double) element.get("Price");
//
//                                if (key == null || price == null) {
//                                    return;
//                                }
//
//                                c.output(KV.of(key, price));
//                            }
//                        }));
//
//        PAssert.that(out).containsInAnyOrder(KV.of("22222", 20.1), KV.of("22222", 20.0));


        String products = "/data/section1/products.csv";
        String productTypes = "/data/section1/product_types.csv";
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

//        Schema joinSchema = Schema.of(
//                Schema.Field.of("ProductId", Schema.FieldType.INT32),
//                Schema.Field.of("ProductName", Schema.FieldType.STRING),
//                Schema.Field.of("Price", Schema.FieldType.INT32),
//                Schema.Field.of("ProductType", Schema.FieldType.STRING)
//        );

        //Inner Join
        PCollection<Row> joinCollection = productTypePCollection
                .apply("Create Join", Join.<Row, Row>innerJoin(productPCollection)
                        .using("ProductTypeId")
                );
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

//        resultCollection
//                .apply(TextIO
//                        .write().withoutSharding()
//                        .to("result/join_products")
//                        .withHeader("ProductId, ProductName, Price, ProductType")
//                        .withSuffix(".csv"));
//        Row row = Row.withSchema(joinSchema)
//                .withFieldValue("ProductId", 1)
//                .withFieldValue("ProductName", "chair")
//                .withFieldValue("Price", 100)
//                .withFieldValue("ProductType", "furniture")
//                .build();

        PAssert.that(resultCollection).containsInAnyOrder("1,chair,100,furniture", "3,fish tank (small),250,electric", "2,table,200,furniture");
        pipeline.run().waitUntilFinish();
    }
}
