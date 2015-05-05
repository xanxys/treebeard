# treebeard
Proof of concept of automatic tech-tree generation from wikipedia through NLP and various heuristics.

## Structure
* importer: Go program that reads wikipedia XML dump (ones which contain only the latest articles), shard articles to jsons for uploading to Google Cloud Storage
* processor: Cloud Dataflow code that do real things
