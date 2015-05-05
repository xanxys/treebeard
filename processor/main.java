public static void main(String[]args){

        // Create Pipeline options object.
        PipelineOptions options=PipelineOptionsFactory.create();

        // Create the Pipeline with default options.
        Pipeline p=Pipeline.create(options);

        // Apply a root transform, a text file read, to the pipeline.
        p.apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/kinglear.txt"))

        // Apply a ParDo transform to the PCollection resulting from the text file read
        .apply(ParDo.of(new DoFn<String, String>(){
@Override
public void processElement(ProcessContext c){
        String[]words=c.element().split("[^a-zA-Z']+");
        for(String word:words){
        if(!word.isEmpty()){
        c.output(word);
        }
        }
        }
        }))

        // Apply the Count.PerElement transform to the PCollection of text strings resulting from the ParDo
        .apply(Count.<String>perElement())

        // Apply a ParDo transform to format the PCollection of word counts from Count() for output
        .apply(ParDo.of(new DoFn<KV<String, Long>,String>(){
@Override
public void processElement(ProcessContext c){
        c.output(c.element().getKey()+": "+c.element().getValue());
        }
        }))

        // Apply a text file write transform to the PCollection of formatted word counts
        .apply(TextIO.Write.to("gs://my-bucket/counts.txt"));

        p.run();
        }
