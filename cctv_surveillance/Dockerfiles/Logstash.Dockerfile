FROM docker.elastic.co/logstash/logstash:7.9.2

RUN bin/logstash-plugin install logstash-codec-protobuf

