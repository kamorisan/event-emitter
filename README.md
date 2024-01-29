# event emitter
A Python source-to-image application for emitting to an Apache Kafka topic

## Launching on OpenShift

```
oc new-app centos/python-36-centos7~https://github.com/kamorisan/event-emmiter -e KAFKA_BROKERS=my-cluster-kafka-bootstrap.my-kafka.svc:9092 -e KAFKA_TOPIC=my-topic -e RATE=1 --name=emitter
```

You will need to adjust the `KAFKA_BROKERS` and `KAFKA_TOPICS` variables to
match your configured Kafka deployment and desired topic. The `RATE` variable
controls how many messages per second the emitter will send, by default this
is set to 3.

#kafdrop
  export PRJ_NAME=test
  oc process -n $PRJ_NAME -f /Users/kamori/VSCode/WorkSpace/camelk-workshop/devspaces_v1/camelk-ws/provision/openshift/03_amqstreams/02_kafdrop.yaml --param=PJ_NAME=$PRJ_NAME | oc apply -f -
  oc set env dc/kafdrop KAFKA_BROKERCONNECT=my-cluster-kafka-bootstrap.$PRJ_NAME.svc:9092 -n $PRJ_NAME