package com.techteam.beam.main;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;
import java.util.Objects;

public class SideInputJoinExampleMain {
    public static void main(String[] args) {
        sideInputRun();
    }

    private static void sideInputRun() {
        Pipeline pipeline = Pipeline.create();

        String products = "/data/section1/products.csv";
        String productTypes = "/data/section1/product_types.csv";
        PCollection<String> productCollection =
                pipeline.apply(TextIO.read().from(products))
                        .apply("FilterHeader", Filter.by(line ->
                                !line.isEmpty() && !line.contains("ProductId, ProductName, ProductTypeId, Price"))
                        );

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

        PCollectionView<Map<Integer, String>> productTypeMap = productTypeCollection
                .apply(View.asMap());

        PCollection<String> resultCollection = productCollection.apply(ParDo
                .of(new DoFn<String, String>() {
                    @ProcessElement
                    public void process(ProcessContext processContext) {
                        Map<Integer, String> map = processContext.sideInput(productTypeMap);

                        String strings = processContext.element();
                        assert strings!= null;
                        String[] splits = strings.split(",");
                        int key = Integer.parseInt(splits[2].trim());
                        String join = map.get(key);
                        if (Objects.nonNull(join)) {
                            String productType = join.split(",")[1];
                            processContext.output(String.join(",", splits[0], splits[1], splits[3], productType));
                        }
                    }
                }).withSideInputs(productTypeMap));

        resultCollection
                .apply(TextIO
                        .write().withoutSharding()
                        .to("result/side_input_products")
                        .withHeader("ProductId, ProductName, Price, ProductType")
                        .withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
}
