import threading
import time
from flask import Flask, render_template, request
from turbo_flask import Turbo
from socket_client import get_socket_message
from colab import create_db, analysis_iteration
import pygal
from constants import *
from pygal.style import Style

app = Flask(__name__)
turbo = Turbo(app)

# create_db()

f = open("hashes.csv", "w")
f.write(",0\n0,2")
f.close()

query = ""

custom_style = Style(
  legend_font_size=20,
  major_label_font_size=20,
  label_font_size=20,
  tooltip_font_size=20,
  title_font_size=20,
)


@app.context_processor
def inject_load():
    data = get_socket_message(query=query)
    
    #gas chart
    bar_chart = pygal.Bar(width=700, legend_box_size=30, style=custom_style)
    bar_chart.title = 'Gas used by transaction type'
        
    for i in range(data[0].shape[0]):
        row_data = list(data[0].iloc[i])
        bar_chart.add(row_data[0], row_data[1])
    
    gas_data=bar_chart.render_data_uri()
    
    #txn value chart
    bar_chart = pygal.Bar(width=500, legend_box_size=30, style=custom_style, show_legend=False)
    bar_chart.title = 'Transaction value used by transaction type'
        
    for i in range(data[0].shape[0]):
        row_data = list(data[0].iloc[i])
        bar_chart.add(row_data[0], row_data[2])
    
    txn_fee_data=bar_chart.render_data_uri()
    
    #usd fee chart
    bar_chart = pygal.Bar(width=500, legend_box_size=30, style=custom_style, show_legend=False)
    bar_chart.title = 'USD fee used by transaction type'
        
    for i in range(data[0].shape[0]):
        row_data = list(data[0].iloc[i])
        bar_chart.add(row_data[0], row_data[4])
    
    usd_fee_data=bar_chart.render_data_uri()

    return {"total_data": data[1], "gas_chart": gas_data, "txn_fee_chart": txn_fee_data, "usd_fee_chart": usd_fee_data}


@app.route('/', methods=['GET', 'POST'])
def index():
    global query
    if request.method == 'POST':
        if request.form.get('Filter') == 'Filter':
            print("Filtering")
            query = ""
            # query = f"SELECT * FROM {table_name} WHERE TxnHash='0xd985d71e004f10e1034d5c152dea402496509d6f2b2643e215b7766676d5eee0'"
            
        elif request.form.get('Reset') == 'Reset':
            print("Resetting")
            query = ""    
    return render_template('index.html')

# @app.route('/template')
# def dashboard():
#     return render_template('index.html')

# @app.route('/set-query')
# def set_query():
#     print("Updating query")
#     global query
#     query = f"SELECT * FROM {table_name} WHERE Age='12 secs ago'"
#     return ("nothing")


def update_load():
    with app.app_context():
        while True:
            time.sleep(20)
            turbo.push(turbo.replace(
                render_template('transactions.html'), 'load'))


th = threading.Thread(target=update_load)
th.daemon = True
th.start()

