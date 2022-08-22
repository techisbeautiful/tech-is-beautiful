package com.techteam.beam;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Serializable;

public class TestPipelineExample implements Serializable {
    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void myPipelineTest() {
        ImmutableList<TableRow> input = ImmutableList.of(
                new TableRow()
                        .set("UserID", "22222")
                        .set("Price", 20.1),
                new TableRow()
                        .set("UserID", "22222")
                        .set("Price", 20.0));

        PCollection<KV<String, Double>> out = p
                .apply("Create input", Create.of(input))
                .apply("Parse pipeline",
                        ParDo.of(new DoFn<TableRow, KV<String, Double>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {

                                TableRow element = c.element();
                                // Check if the values returned from BigQuery are null, as all columns in BigQuery are nullable
                                String key = element.get("UserID") == null ? null : (String) element.get("UserID");
                                Double price = element.get("Price") == null ? null : (Double) element.get("Price");

                                if (key == null || price == null) {
                                    return;
                                }

                                c.output(KV.of(key, price));
                            }
                        }));

        PAssert.that(out).containsInAnyOrder(KV.of("22222", 20.1), KV.of("22222", 20.0));
        p.run().waitUntilFinish();
    }
}
