//package com.techteam.beam;
//
//import com.google.api.services.bigquery.model.TableRow;
//import com.techteam.beam.bq.entities.Votes;
//import com.techteam.beam.extract.TableRowToRecordFn;
//import com.techteam.beam.transform.TableRowToRecord;
//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.beam.sdk.coders.AvroCoder;
//import org.apache.beam.sdk.coders.StringUtf8Coder;
//import org.apache.beam.sdk.testing.PAssert;
//import org.apache.beam.sdk.testing.TestPipeline;
//import org.apache.beam.sdk.testing.ValidatesRunner;
//import org.apache.beam.sdk.transforms.Create;
//import org.apache.beam.sdk.transforms.MapElements;
//import org.apache.beam.sdk.transforms.ParDo;
//import org.apache.beam.sdk.values.PCollection;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.experimental.categories.Category;
//import org.junit.runner.RunWith;
//import org.junit.runners.JUnit4;
//
//import java.io.File;
//import java.util.Arrays;
//import java.util.List;
//
//@RunWith(JUnit4.class)
//public class ReadBigQueryTest {
//    private static final String ID = "id";
//    private static final String CREATION_DATE = "creation_date";
//    private static final String POST_ID = "post_id";
//    private static final String VOTE_TYPE_ID = "vote_type_id";
//
//    @Rule
//    public TestPipeline p = TestPipeline.create();

//    @Test
//    public void testTableRowToRecordFn() throws Exception {
//        String schemaPath = "D:/working/projects/tech_team/stackoverflow-bq-to-gs/src/main/resources/schema/entity/votes.avro";
//        TableRow row1 = new TableRow();
//        row1.set(ID, "1");
//        row1.set(CREATION_DATE, "2019-10-28 00:00:00 UTC");
//        row1.set(POST_ID, "1");
//        row1.set(VOTE_TYPE_ID, "1");
//
//        TableRow row2 = new TableRow();
//        row2.set(ID, "2");
//        row2.set(CREATION_DATE, "2019-10-29 00:00:00 UTC");
//        row2.set(POST_ID, "2");
//        row2.set(VOTE_TYPE_ID, "2");
//        List<TableRow> rows = Arrays.asList(row1, row2);
//        PCollection<GenericRecord> output =
//                p.apply(Create.of(rows))
//                        .apply(ParDo.of(new TableRowToRecordFn(new Votes(), schemaPath))).setCoder(AvroCoder.of(GenericRecord.class, new Schema.Parser().parse(new File(schemaPath))));
//
//        GenericRecord record = new GenericData.Record(new Schema.Parser().parse(new File(schemaPath)));
//        record.put(ID, "1");
//        record.put(CREATION_DATE, "2019-10-28 00:00:00 UTC");
//        record.put(POST_ID, "1");
//        record.put(VOTE_TYPE_ID, "1");
//
//        GenericRecord record2 = new GenericData.Record(new Schema.Parser().parse(new File(schemaPath)));
//        record2.put(ID, "2");
//        record2.put(CREATION_DATE, "2019-10-29 00:00:00 UTC");
//        record2.put(POST_ID, "2");
//        record2.put(VOTE_TYPE_ID, "2");
//
//        PAssert.that(output).containsInAnyOrder(record, record2);
//        p.run().waitUntilFinish();
//    }
//
//    @Test
//    @Category(ValidatesRunner.class)
//    public void testTableRowToRecord() throws Exception {
//        String schemaPath = "D:/working/projects/tech_team/stackoverflow-bq-to-gs/src/main/resources/schema/entity/votes.avro";
//        TableRow row1 = new TableRow();
//        row1.set(ID, "1");
//        row1.set(CREATION_DATE, "2019-10-28 00:00:00 UTC");
//        row1.set(POST_ID, "1");
//        row1.set(VOTE_TYPE_ID, "1");
//
//        TableRow row2 = new TableRow();
//        row2.set(ID, "2");
//        row2.set(CREATION_DATE, "2019-10-29 00:00:00 UTC");
//        row2.set(POST_ID, "2");
//        row2.set(VOTE_TYPE_ID, "2");
//
//        GenericRecord record = new GenericData.Record(new Schema.Parser().parse(new File(schemaPath)));
//        record.put(ID, "1");
//        record.put(CREATION_DATE, "2019-10-28 00:00:00 UTC");
//        record.put(POST_ID, "1");
//        record.put(VOTE_TYPE_ID, "1");
//
//        GenericRecord record2 = new GenericData.Record(new Schema.Parser().parse(new File(schemaPath)));
//        record2.put(ID, "2");
//        record2.put(CREATION_DATE, "2019-10-29 00:00:00 UTC");
//        record2.put(POST_ID, "2");
//        record2.put(VOTE_TYPE_ID, "2");
//
//        List<TableRow> rows = Arrays.asList(row1, row2);
//
//        PCollection<GenericRecord> output =
//                p.apply(Create.of(rows))
//                        .apply(new TableRowToRecord(new Votes(), schemaPath));
//
//        PAssert.that(output).containsInAnyOrder(record, record2);
//        p.run().waitUntilFinish();
//    }
//}
