# Kafka Write JSON to GCS Tutorial
Apache Beam streaming application from Kafka, transform the record, and store it to GCS with JSON format tutorial.

## Command to Execute
```bash
java -jar <jar_name> \
    --project=<project_id> \
    --tempLocation=<temp_location> \
    --runner=DirectRunner \
    --bootstrapServers=<bootstrap_servers> \
    --groupId=<group_id> \
    --topic=<topic> \
    --autoOffsetReset=<earliest/latest> \
    --windowDuration=<5s/5m/5h> \
    --numShards=<number_of_shards> \
    --outputDirectory=<output>
```
