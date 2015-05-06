package net.xanxys.treebeard.processor;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.List;

public class WikiLinkExtractor {
    /**
     * Options supported by {@link WikiLinkExtractor}.
     * <p>
     * Inherits standard configuration options.
     */
    public static interface Options extends PipelineOptions {
        @Description("Wikipedia articles table")
        @Default.String("xanxys-treebeard:wikipedia.articles")
        String getInput();

        void setInput(String value);

        @Description("Path of the file to write to")
        @Default.String("techs.wp_links")
        String getOutput();

        void setOutput(String value);
    }

    public static void main(String[] args) {
        final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        final Pipeline extractor = Pipeline.create(options);

        final PCollection<TableRow> articles = extractor
                .apply(BigQueryIO.Read.named("WikiArticles").from(options.getInput()));
        final PCollection<TableRow> links = articles.apply(ParDo.of(new ExtractLinksFn()));

        // Resolve link title by joining with articles. (use title as keys)
        // Output links with article_id_dst attached.
        final TupleTag<String> tagArticle = new TupleTag<>();
        final TupleTag<TableRow> tagLink = new TupleTag<>();

        final PCollection<TableRow> resolvedLinks =
                KeyedPCollectionTuple
                        .of(tagArticle,
                                articles.apply(ParDo.of(new DoFn<TableRow, KV<String, String>>() {
                                    @Override
                                    public void processElement(ProcessContext c) throws Exception {
                                        c.output(KV.of((String) c.element().get("Title"), (String) c.element().get("ArticleId")));
                                    }
                                })))
                        .and(tagLink,
                                links.apply(ParDo.of(new DoFn<TableRow, KV<String, TableRow>>() {
                                    @Override
                                    public void processElement(ProcessContext c) throws Exception {
                                        c.output(KV.of((String) c.element().get("link_title"), c.element()));
                                    }
                                })))
                        .apply(CoGroupByKey.<String>create())
                        .apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
                            private Aggregator<Long> countLinkTitleNotFound;
                            private Aggregator<Long> countLinkTitleAmbiguous;

                            @Override
                            public void startBundle(Context c) throws Exception {
                                super.startBundle(c);
                                countLinkTitleNotFound = c.createAggregator("link_title_not_found", new Sum.SumLongFn());
                                countLinkTitleAmbiguous = c.createAggregator("link_title_ambiguous", new Sum.SumLongFn());
                            }

                            @Override
                            public void processElement(ProcessContext c) throws Exception {
                                final KV<String, CoGbkResult> kv = c.element();

                                final ArrayList<String> ids = new ArrayList<String>();
                                for(String id : kv.getValue().getAll(tagArticle)) {
                                    ids.add(id);
                                }
                                for (TableRow link : kv.getValue().getAll(tagLink)) {
                                    if (ids.size() == 0) {
                                        countLinkTitleNotFound.addValue(1L);
                                    } else {
                                        link.set("article_id_dst", ids.get(0));
                                        if (ids.size() > 1) {
                                            countLinkTitleAmbiguous.addValue(1L);
                                        }
                                        c.output(link);
                                    }
                                }
                            }
                        }));

        // Prepare output schema.
        final List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("article_id_src").setType("STRING"));
        fields.add(new TableFieldSchema().setName("article_id_dst").setType("STRING"));
        fields.add(new TableFieldSchema().setName("link_label").setType("STRING"));
        fields.add(new TableFieldSchema().setName("link_title").setType("STRING"));
        fields.add(new TableFieldSchema().setName("redirect").setType("BOOLEAN"));
        final TableSchema schema = new TableSchema().setFields(fields);

        resolvedLinks.apply(BigQueryIO.Write
                .named("WriteLinks")
                .to(options.getOutput())
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
        extractor.run();
    }

}
