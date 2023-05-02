import random
import threading
import time
from flask import Flask, render_template
from turbo_flask import Turbo
from socket_client import get_socket_message

app = Flask(__name__)
turbo = Turbo(app)


@app.context_processor
def inject_load():
    data = get_socket_message()
    # print("received data:", data)
    return {"data":data}


@app.route('/')
def index():
    return render_template('index.html')


def update_load():
    with app.app_context():
        while True:
            time.sleep(1)
            turbo.push(turbo.replace(render_template('transactions.html'), 'load'))


th = threading.Thread(target=update_load)
th.daemon = True
th.start()