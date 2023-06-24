#!/bin/bash
## to run this you must start using CLI ./kafka_startup.sh
# and change permissions to execute chmod +x kafka_startup.sh


echo "Changing Directory to kafka"
cd ../kafka


echo "Deleting Kafka Consumer Info "
docker exec kafka2 /bin/sh -c kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group <consumer-group>


kafka-topics --bootstrap-server localhost:9092 --list --command-config /etc/kafka/secrets/client.properties

kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete list --group 



echo "Creating Topic"
docker exec kafka2 /bin/sh -c "/bin/kafka-topics --create --replication-factor 1 --partitions 1 --topic ticketmaster --bootstrap-server localhost:9092"

echo "Created Topic.. Listing Topics"
docker exec kafka2 /bin/sh -c "/bin/kafka-topics --list --bootstrap-server localhost:29092"

exit

# delete the docker container
# docker-compose -p kafka_test_1 down
