package net.xanxys.treebeard.processor;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class WikiLinkExtractor {
    /**
     * A DoFn that tokenizes lines of text into individual words.
     */
    static class ExtractWordsFn extends DoFn<String, String> {
        private static final long serialVersionUID = 0;

        private Aggregator<Long> emptyLines;

        @Override
        public void startBundle(Context c) {
            emptyLines = c.createAggregator("emptyLines", new Sum.SumLongFn());
        }

        @Override
        public void processElement(ProcessContext c) {
            // Keep track of the number of empty lines. (When using the [Blocking]DataflowPipelineRunner,
            // Aggregators are shown in the monitoring UI.)
            if (c.element().trim().isEmpty()) {
                emptyLines.addValue(1L);
            }

            // Split the line into words.
            String[] words = c.element().split("[^a-zA-Z']+");

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    c.output(word);
                }
            }
        }
    }

    /**
     * A DoFn that converts a Word and Count into a printable string.
     */
    static class FormatCountsFn extends DoFn<KV<String, Long>, String> {
        private static final long serialVersionUID = 0;

        @Override
        public void processElement(ProcessContext c) {
            c.output(c.element().getKey() + ": " + c.element().getValue());
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     * <p>
     * Although this pipeline fragment could be inlined, bundling it as a PTransform allows for easy
     * reuse, modular testing, and an improved monitoring experience.
     */
    public static class CountWords extends PTransform<PCollection<String>, PCollection<String>> {
        private static final long serialVersionUID = 0;

        @Override
        public PCollection<String> apply(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(
                    ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts =
                    words.apply(Count.<String>perElement());

            // Format each word and count into a printable string.
            PCollection<String> results = wordCounts.apply(
                    ParDo.of(new FormatCountsFn()));

            return results;
        }
    }

    /**
     * Options supported by {@link WordCount}.
     * <p>
     * Inherits standard configuration options.
     */
    public static interface Options extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
        String getInput();

        void setInput(String value);

        @Description("Path of the file to write to")
        @Default.InstanceFactory(OutputFactory.class)
        String getOutput();

        void setOutput(String value);

        /**
         * Returns gs://${STAGING_LOCATION}/"counts.txt" as the default destination.
         */
        public static class OutputFactory implements DefaultValueFactory<String> {
            @Override
            public String create(PipelineOptions options) {
                DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
                if (dataflowOptions.getStagingLocation() != null) {
                    return GcsPath.fromUri(dataflowOptions.getStagingLocation())
                            .resolve("counts.txt").toString();
                } else {
                    throw new IllegalArgumentException("Must specify --output or --stagingLocation");
                }
            }
        }

        /**
         * By default (numShards == 0), the system will choose the shard count.
         * Most programs will not need this option.
         */
        @Description("Number of output shards (0 if the system should choose automatically)")
        int getNumShards();

        void setNumShards(int value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
                .apply(new CountWords())
                .apply(TextIO.Write.named("WriteCounts")
                        .to(options.getOutput())
                        .withNumShards(options.getNumShards()));

        p.run();
    }

}
