import json
from flask import Flask, Response, render_template, url_for, redirect
import obj_str_access as os_access
import event_streams_access as es_access

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('home.html')

@app.route('/services')
def services():
    service_data = [] # list of json objects that the service will hold
    curr_tracker = os_access.get_item('mc-objstore', 'Curr_trkr.json').decode().replace("'", '"')
    # service_data.append(json.loads(json_string))

    mark_inx_trkr = os_access.get_item('mc-objstore', 'mark_inx_trkr.json').decode().replace("'", '"')
    # service_data.append(json.loads(json_string))
    return render_template('services.html', curr_tracker=json.loads(curr_tracker), mark_inx_trkr=json.loads(mark_inx_trkr))

@app.route('/refresh-service', methods=['GET'])
def refresh_service(service_name=None):
    if service_name == None:
        os_access.get_item('mc-objstore', 'mark_inx_trkr.json')
        
        return redirect(url_for('services'))
    # when the refresh button is pushed, update only industry perf
    # click on refresh, message is sent to event streams to up

    # set up service as serverless function that runs when a message gets popped?
    return redirect(url_for('services'))

if __name__ == '__main__':
    app.run(debug=True)
