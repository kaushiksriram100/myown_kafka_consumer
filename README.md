# myown_kafka_consumer
just a wrapper to consumer messages from kafka and print the partitions consumed from. 

like this: 
./kafka_consumer_mac_build --topic topic_name|grep -i "topic_name\[24\]" --bootstrap-servers "list:9092"

Facing some issue cross compiling (for linux/amd64). Works fine for mac local build. 
