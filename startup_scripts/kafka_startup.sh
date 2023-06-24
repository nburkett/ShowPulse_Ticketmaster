#!/bin/bash
## to run this you must start using CLI ./kafka_startup.sh
# and change permissions to execute chmod +x kafka_startup.sh


echo "Building kafka docker images..."
cd ../kafka

echo "Creating Kafka Container"
## give a new name 
docker-compose -p kafka_test_1 up -d

## manually 
# docker exec -it kafka /bin/bash

echo "Changing Directory to kafka"
docker exec kafka /bin/sh -c "cd opt/kafka/bin/"

echo "Creating Topic"
docker exec kafka /bin/sh -c "kafka-topics.sh --create --replication-factor 1 --partitions 1 --topic ticketmaster --bootstrap-server localhost:9092"

echo "Created Topic.. Listing Topics"
docker exec kafka /bin/sh -c "kafka-topics.sh --list --bootstrap-server localhost:9092"

exit


# docker exec -it kafka /bin/sh

#list consumer groups
# kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list



# delete the docker container
# docker-compose -p kafka_test_1 down

# /bin/kafka-consumer-groups.sh \
#            --bootstrap-server localhost:9092 \
#            --list