
printf "\n##### Deploy Kafka Operator...\n"

kubectl create ns kafka

sed -i 's/namespace: .*/namespace: kafka/' kafka/strimzi-0.29.0/install/cluster-operator/*RoleBinding*.yaml

kubectl create ns my-kafka-project

kubectl create -f kafka/strimzi-0.29.0/install/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml -n my-kafka-project --validate=false
kubectl create -f kafka/strimzi-0.29.0/install/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml -n my-kafka-project --validate=false
kubectl create -f kafka/strimzi-0.29.0/install/cluster-operator/ -n kafka --validate=false

kubectl create -n my-kafka-project -f kafka/kafka-cluster.yaml --validate=false

kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n my-kafka-project

kubectl create -n my-kafka-project -f kafka/kafka-input-topic.yaml
kubectl create -n my-kafka-project -f kafka/kafka-output-topic.yaml
kubectl create -n my-kafka-project -f kafka/kafka-db-topic.yaml



