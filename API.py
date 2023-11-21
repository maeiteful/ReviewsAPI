from flask import Flask, request, jsonify, redirect
import subprocess
import threading
import queue
import schedule
import requests
import logging
from updatereview import Reviews_main

app = Flask(__name__)
request_queue = queue.Queue()
lock = threading.Lock()

app.logger.setLevel(logging.DEBUG)  # Set the desired log level
handler = logging.FileHandler("flask_app.log")  # Specify the path to your Flask app log file
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
app.logger.addHandler(handler)



def process_queue():
    while True:
        try:
            data = request_queue.get()
        except:
            None
        try:
            id =data["client_id"]
            Reviews_main(None,id)

            # Respond to the request
            response_data = {"Message": "Success!"}
            data['response_queue'].put(response_data)
        except Exception as e:
            response_data = {'error': str(e)}
            print(response_data)
            data['response_queue'].put(response_data)

        finally:
            request_queue.task_done()

def start_worker_thread():
    worker_thread = threading.Thread(target=process_queue)
    worker_thread.daemon = True
    worker_thread.start()

# Call the function to start the worker thread when the Flask app is created
start_worker_thread()

def ping_self():
    app.logger.info("Pinging myself...")
    print("Pinging myself...")
    # Make a request to a route in your Flask app
    response = requests.get("http://lsa.clixsy.com:54323/ping")  
    print("Response:", response.text)
    app.logger.info("Response:", response.text)

# Schedule the ping task every hour (adjust as needed)
schedule.every().hour.do(ping_self)

@app.route('/ping')
def ping():
    app.logger.info("Pinged")
    return 'Pinged!'

@app.errorhandler(Exception)
def handle_exception(e):
    response = {
            "error": {
                "code": 500,
                "message": "Internal Server Error"
            }
            }
    return jsonify(response), 500

@app.route('/update_reviews', methods=['POST','GET'])
def run_script():
    if request.method == 'GET' or request.method == 'POST':
        print("Get Request Recieved")
        app.logger.info("Get Request Recieved")

        try:
            
            # Get the JSON data from the request
            data = request.get_json()
            try:
                test=data["client_id"]
            except:
                response = {
            "error": {
                "message": "No client ID"
            }
            }
                return response
            response_queue = queue.Queue()
            data['response_queue'] = response_queue
            
            request_queue.put(data)
            #response_data = response_queue.get()
            #response_data = process_queue2(data)
            response = {
                "error": {
                    "code": 200,
                    "message": "ok"
                }
            }

            return jsonify(response)

        except Exception as e:
            response = {
            "error": {
                "code": 500,
                "message": "Internal Server Error"
            }
            }
            return jsonify(response), 500




if __name__ == '__main__':
    '''
    worker_thread = threading.Thread(target=process_queue)
    worker_thread.daemon = True
    worker_thread.start()
    host='0.0.0.0', port=54324'''
    app.run(host='0.0.0.0', port=54323,threaded=False)

