package com.techteam.beam.main;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class SelectExample {
    public static void main(String[] args) {
        runGroupQuery();
    }

    private static void runGroupQuery() {
        String products = "/data/section1/products.csv";
        Pipeline pipeline = Pipeline.create();

        Schema productSchema = Schema.of(
                Schema.Field.of("ProductId", Schema.FieldType.INT32),
                Schema.Field.of("ProductName", Schema.FieldType.STRING),
                Schema.Field.of("ProductTypeId", Schema.FieldType.INT32),
                Schema.Field.of("Price", Schema.FieldType.INT32)
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

        PCollection<String> selectPCollection = productPCollection
                .apply(Select.fieldNames("ProductId", "ProductName", "Price"))
                .apply(ParDo.of(new DoFn<Row, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Row row = c.element();
                        assert row != null;
                        c.output(String.join(",",
                                checkValue(row, 0),
                                Objects.requireNonNull(checkValue(row, 1)).toString().toUpperCase(),
                                checkValue(row, 2),
                                new SimpleDateFormat("YYYYMMDD").format(new Date())
                        ));
                    }
                    private CharSequence checkValue(Row row, int index) {
                        return Objects.isNull(row) ? null : row.getValue(index) + "";
                    }
                }));

        selectPCollection
                .apply(TextIO
                        .write().withoutSharding()
                        .to("result/select_products")
                        .withHeader("ProductId, ProductName, Price, InsertedDate")
                        .withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
}
