"""
 Copyright 2015-2018 IBM
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 Licensed Materials - Property of IBM
 © Copyright IBM Corp. 2015-2018
"""
import asyncio
import sys
from confluent_kafka import Producer, Consumer, KafkaException

class ProducerTask(object):
    def __init__(self, conf, topic_name):
        self.topic_name = topic_name
        self.producer = Producer(conf)
        self.counter = 0
        self.running = True

    def stop(self):
        self.running = False

    def on_delivery(self, err, msg):
        if err:
            print('Delivery report: Failed sending message {0}'.format(msg.value()))
            print(err)
            # We could retry sending the message
        else:
            print('Message produced, offset: {0}'.format(msg.offset()))

    def run(self):
        self.producer.produce(self.topic_name, "Hi Mike!", callback=self.on_delivery)
        self.producer.poll(0)

        self.producer.flush()

class ConsumerTask(object):

    def __init__(self, conf, topic_name):
        self.consumer = Consumer(conf)
        self.topic_name = topic_name
        self.running = True

    def stop(self):
        self.running = False

    def print_assignment(self, consumer, partition):
        print('Assignment: ', partition)

    def run(self):
        self.consumer.subscribe([self.topic_name], on_assign=self.print_assignment)
        
        try:
            while True:
                msg = self.consumer.poll(10)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    print('%% %s [%d] at offset %d with key %s:\n' %
                                    (msg.topic(), msg.partition(), msg.offset(),
                                    str(msg.key())))
                    print(msg.value())
        except KeyboardInterrupt:
            sys.stderr.write("%% Aborted by user\n")
        finally:
            self.consumer.unsubscribe()
            self.consumer.close()


class EventStreamsDriver(object):
    def __init__(self, topic_name, service_name, producer):
        self.consumer = None
        self.producer = None

        if producer:
            self.run_producer = True
        else:
            self.run_producer = False
        
        self.topic_name = topic_name
        self.base_config = {
            'bootstrap.servers': 'broker-1-9tl582p7src9jz2d.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093,broker-5-9tl582p7src9jz2d.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093,broker-4-9tl582p7src9jz2d.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093,broker-2-9tl582p7src9jz2d.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093,broker-3-9tl582p7src9jz2d.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093,broker-0-9tl582p7src9jz2d.kafka.svc03.us-south.eventstreams.cloud.ibm.com:9093',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': 'token',
            'sasl.password': 'nbHuMTLnLi-rmmhP22gUQSUuExarXsEpF8z49FSBLJRj',
            'api.version.request': True,
            'broker.version.fallback': '0.10.2.1',
            'log.connection.close' : False
        }
        self.prod_config = {
            'client.id': service_name + '-producer'
        }
        self.cons_config = {
            'client.id': service_name + '-consumer',
            'group.id': 'money-cloud-services'
        }

        for key in self.base_config:
            self.cons_config[key] = self.base_config[key]
            self.prod_config[key] = self.base_config[key]

    def run_task(self):
        # tasks = []
        if self.run_producer:
            self.producer = ProducerTask(self.prod_config, self.topic_name)
            self.producer.run()
            # tasks.append(asyncio.ensure_future(self.producer.run()))
        else:
            self.consumer = ConsumerTask(self.cons_config, self.topic_name)
            self.consumer.run()
            # tasks.append(asyncio.ensure_future(self.consumer.run()))
        
        # done, pending = yield from asyncio.wait(tasks)
        # for future in done | pending:
        #     future.result()
        # sys.exit(0)

access = EventStreamsDriver('Market-Idx', 'Market-Idx', True)
access.run_task()
