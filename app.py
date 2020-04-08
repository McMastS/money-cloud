import json
from flask import Flask, Response, render_template, url_for, redirect, request
import obj_str_access as os_access
import event_streams_access as es_access
from pusher import Pusher
from confluent_kafka import Producer, Consumer, KafkaException
import sys



app = Flask(__name__)
pusher = Pusher(app_id=u'976708', key=u'46dce6ce5257370ecd3e', secret=u'8ea438504adf84b50776', cluster=u'us2')


@app.route('/')
def index():
    return render_template('home.html')


@app.route('/push')
def push():
	return render_template('push.html')


@app.route('/services')
def services():
    a = listen_eventMessage()
    
    service_data = [] # list of json objects that the service will hold
    curr_tracker = os_access.get_item('mc-objstore', 'Curr_trkr.json').decode().replace("'", '"')
    # service_data.append(json.loads(json_string))
    mark_inx_trkr = os_access.get_item('mc-objstore', 'mark_inx_trkr.json').decode().replace("'", '"')
    # service_data.append(json.loads(json_string))

    perf_forecast = os_access.get_item('mc-objstore', 'Performance_Forcast.json').decode().replace("'", '"')
    
    return render_template('services.html', curr_tracker=json.loads(curr_tracker), 
                            mark_inx_trkr=json.loads(mark_inx_trkr), perf_forecast=json.loads(perf_forecast), topic = a.topic(), message = a.value().decode())

@app.route('/refresh-service', methods=['POST'])
def refresh_service(service_name=None):
    if service_name == None:
        os_access.get_item('mc-objstore', 'mark_inx_trkr.json')
        
        return redirect(url_for('services'))
    # when the refresh button is pushed, update only industry perf
    # click on refresh, message is sent to event streams to up

    # set up service as serverless function that runs when a message gets popped?
    return redirect(url_for('services'))

@app.route('/dashboard')
def dashboard():
	return render_template('dashboard.html')

@app.route('/message', methods=['POST'])
def message():
	data = request.form
	pusher.trigger(u'message', u'send', {
		u'name': data['name'],
		u'message': data['message']
	})
	return "message sent"

if __name__ == '__main__':
    app.run(debug=True)


def listen_eventMessage():
    driver = EventStreamsDriver('Market-Idx','Market-Idx',False)
    msg = driver.run_task()
    return msg
class ConsumerTask(object):

    def __init__(self, conf, topic_name):
        self.consumer = Consumer(conf)
        self.topic_name = topic_name
        self.running = True
        self._observers = []

    def stop(self):
        self.running = False

    def print_assignment(self, consumer, partition):
        print('Assignment - subscribing to topic: ', partition)

    def register_observer(self, observer):
        self._observers.append(observer)

    def notify_observers(self, *args, **kwargs):
        for observer in self._observers:
            observer.notify(self, *args, **kwargs)

    def run(self):
        self.consumer.subscribe([self.topic_name], on_assign=self.print_assignment)

        try:
            while True:
                msg = self.consumer.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                    (msg.topic(), msg.partition(), msg.offset(),
                                    str(msg.key())))
                    #print(msg.value())
                    self.notify_observers(msg.topic())
                    return msg
                    #could add something here that will tell the widget / UI to go to Object Storage
        except KeyboardInterrupt:
            sys.stderr.write("%% Aborted by user\n")
        finally:
            self.consumer.unsubscribe()
            self.consumer.close()


#code creates an instance of eventstreamsdriver
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
            print(True)
            # tasks.append(asyncio.ensure_future(self.producer.run()))
            return null
        else:
            self.consumer = ConsumerTask(self.cons_config, self.topic_name)
            observer=Observer(self.consumer)
            msg = self.consumer.run()
            return msg
#in conjunction with the code in consumertask and eventstreamsdriver, Observer allows 'notify' upon reciept of a message and triggers our action
class Observer(object):
    def __init__(self, ConsumerTask):
        ConsumerTask.register_observer(self)

    def notify(self, ConsumerTask, *args, **kwargs):
        #print('Got', args, kwargs, 'From', ConsumerTask)
        if(args[0]=='Market-Idx'):
            print("Message Recieved from: ", args[0])
            print()
        
            print()
            print("-------------------------------------------------------")