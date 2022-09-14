from typing import ValuesView
import kafka
import json
import flask
from flask import Flask

# setup the consumer
cons = kafka.KafkaConsumer(
    "transactions", 
    bootstrap_servers="localhost:9092", 
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id="log_consumers",
    client_id="Peter")



event_count = 0
add_to_card_count = 0


# start receiving messages
for message in cons:
    print(message.value)
    event_count += 1
    if message.value["event"] == "addtocart":
        add_to_card_count += 1


    # calculate statistics, in this case the addtocard/event ratio
    print(add_to_card_count/event_count)



