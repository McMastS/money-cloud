from pykafka import KafkaClient

client = KafkaClient(hosts="localhost:9092")

topic = client.topics['testBusdata']

producer = topic.get_sync_producer()

count = 1

while count < 10:
    message = ("hello-" + str(count))