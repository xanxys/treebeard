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
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikiLinkExtractor {
    static class ExtractLinksFn extends DoFn<TableRow, TableRow> {
        private static final long serialVersionUID = 0;
        private Pattern linkRegex;

        @Override
        public void startBundle(Context c) throws Exception {
            super.startBundle(c);
            linkRegex = Pattern.compile("\\[\\[(\\S+)\\]\\]");
        }

        @Override
        public void processElement(ProcessContext c) {
            final String aid = (String) c.element().get("ArticleId");
            final String text = (String) c.element().get("Text");

            final Matcher matcher = linkRegex.matcher(text);

            while (matcher.find()) {
                c.output(new TableRow()
                        .set("article_id", aid)
                        .set("link_text", matcher.group(1)));

            }
        }
    }

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

        // Prepare output schema.
        final List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("article_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("link_text").setType("STRING"));
        final TableSchema schema = new TableSchema().setFields(fields);

        extractor
                .apply(BigQueryIO.Read.named("WikiArticles").from(options.getInput()))
                .apply(ParDo.of(new ExtractLinksFn()))
                .apply(BigQueryIO.Write
                        .named("WriteLinks")
                        .to(options.getOutput())
                        .withSchema(schema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
        extractor.run();
    }

}
