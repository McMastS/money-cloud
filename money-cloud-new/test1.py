import json
from flask import Flask, Response, render_template, url_for, redirect, request
import obj_str_access as os_access
import event_streams_access as es_access
from pusher import Pusher

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
    service_data = [] # list of json objects that the service will hold
    driver = EventStreamsDriver('Market-Idx', 'Market-Idx', False)
    a = driver.run_task()
    curr_tracker = os_access.get_item('mc-objstore', 'Curr_trkr.json').decode().replace("'", '"')
    # service_data.append(json.loads(json_string))
    mark_inx_trkr = os_access.get_item('mc-objstore', 'mark_inx_trkr.json').decode().replace("'", '"')
    # service_data.append(json.loads(json_string))

    perf_forecast = os_access.get_item('mc-objstore', 'Performance_Forcast.json').decode().replace("'", '"')
    return render_template('services.html', curr_tracker=json.loads(curr_tracker), 
                            mark_inx_trkr=json.loads(mark_inx_trkr), perf_forecast=json.loads(perf_forecast))

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
