import numpy as np
import time
import kafka
import json
import pandas as pd

# load in all the data at once
data = pd.read_csv("~/Documents/code/kafka/events.csv").sort_values("timestamp", ascending=True)

# create the kafka producer
prod = kafka.KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers='localhost:9092')


# timestamp is in milliseconds
time_diff = data["timestamp"].diff()/1000


# create an artificial streaming process by sending messages to the topic and waiting a random amount of time for n time steps.
def generate_simple(n=3, speedup=1):
    """
    creates messages from the dataset and sends them to the topic.
    n: number of messages to send.
    speedup: how much to speed up the message sending process over normal time.
    """

    for i in range(n):

        # send message in chronological order
        prod.send("general_logs", data.iloc[i].to_dict())

        if data.iloc[i]["event"] == "addtocart":
            prod.send("transactions", data.iloc[i].to_dict())

        # wait for the time between messages.
        time.sleep(time_diff.iloc[i+1]/speedup)


if __name__ == "__main__":
    generate_simple(1000, speedup=20)