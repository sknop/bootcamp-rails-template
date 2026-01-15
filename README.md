# bootcamp-rails-template
Template files for Kafka Streams development with Maven and model answers

You need to run

    mvn io.confluent:kafka-schema-registry-maven-plugin:download

first to download the schema.

To be able to do that, you need to create a `.envrc` file with these options

    export SCHEMA_REGISTRY_URLS=<some URL>
    export USER_INFO_CONFIG=<KEY>:<SECRET>

If you do not use .envrc, set these shell variable by hand in your environment.
