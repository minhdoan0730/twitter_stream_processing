from flask import Flask, jsonify, request
from flask import render_template
import ast
import re

app = Flask(__name__)
labels = []
values = []

@app.route("/")
def get_chart_page():
    global labels, values
    labels = []
    values = []
    return render_template('chart.html', values=values, labels=labels)

@app.route('/refreshData')
def refresh_graph_data():
    global labels, values
    print("labels now: " + str(labels))
    print("data now: " + str(values))
    return jsonify(sLabel=labels, sData=values)

@app.route('/updateData', methods=['POST'])
def update_data():
    global labels, values
    if not request.form or 'data' not in request.form:
        return "error", 400

    label_count_map = {}
    res_labels = labels + ast.literal_eval(request.form['label'])
    res_values = values + ast.literal_eval(request.form['data'])

    [
        label_count_map.__setitem__(
            re.sub(r"[^a-zA-Z0-9\s\#]","", item),
            1 + label_count_map.get(re.sub(r"[^a-zA-Z0-9\s\#]","", item), 0)
        )
        for item in res_labels
    ]

    labels = list(label_count_map.keys())
    values = list(label_count_map.values())

    print("labels received: " + str(labels))
    print("data received: " + str(values))
    return "success", 201

if __name__ == "__main__":
    app.run(host='localhost', port=5000)
