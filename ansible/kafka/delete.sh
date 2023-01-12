kubectl delete -f kafka/strimzi-0.29.0/install/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml -n my-kafka-project
kubectl delete -f kafka/strimzi-0.29.0/install/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml -n my-kafka-project
kubectl delete -f kafka/strimzi-0.29.0/install/cluster-operator/ -n kafka

kubectl delete -n my-kafka-project -f kafka/kafka-cluster.yaml

kubectl delete namespace my-kafka-project
kubectl delete namespace kafka

kubectl delete -n my-kafka-project -f kafka/kafka-input-topic.yaml
kubectl delete -n my-kafka-project -f kafka/kafka-output-topic.yaml
kubectl delete -n my-kafka-project -f kafka/kafka-db-topic.yaml