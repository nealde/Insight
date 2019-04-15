import dash
from dash.dependencies import Output, Input, State
import dash_core_components as dcc
import dash_html_components as html
import redis
import time
import kafka
import json
import requests
from lxml import html as xhtml
import random, string
import numpy as np
from plotly.graph_objs import *


external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(
    [
        html.Div(
            className="app-header",
            children=[
                html.H1("Cloud Compare"),
                html.H3(
                    "An app for text comparison at cloud-scale.  Please submit a question below to test it against the previously recorded Stack Overflow questions!"
                ),
                html.H5("Enter a Stack Overflow link below to find related posts!"),
                dcc.Textarea(
                    placeholder="Enter a Stack Overflow link...",
                    value="",
                    style={"width": "100%", "height": "15"},
                    id="link_text",
                ),
                html.H5("Title"),
                dcc.Textarea(
                    placeholder="Enter a title...",
                    value="",
                    style={"width": "100%", "height": "15"},
                    id="title_text",
                ),
        #    ],
        #),
        html.H5("Question Body:"),
        dcc.Textarea(
            placeholder="Enter a question!",
            value="",
            style={"width": "100%", "height": "180"},
            id="input_text",
        ),
        html.H5("Enter tags to filter by below"),
        dcc.Textarea(
            placeholder="Enter some tags...",
            value="",
            style={"width": "100%", "height": "15"},
            id="tags_text",
        ),
        html.Button("Submit", id="button"),
        html.Button("Scrape", id="button-test"),
        html.Div(["Test output will be reported here"], id="output"),
        ], style={'width':'45%','display': 'inline-block'},
    ),
    html.Div(children=[
        html.H3('Strain the system using buttons below:'),
        html.Button('Strain Kafka', id='kafka-test'),
        html.Button('Strain Pipeline',id='pipe-test'),
        html.H5('Kafka Throughput:'),
        html.Div([
            dcc.Graph(id='kafka'),
        ], className='twelve columns'),
        html.H5("Pipeline Throughput:"),
        html.Div([
            dcc.Graph(id='pipeline'),
        ], className='twelve columns'),
        dcc.Interval(id='update', interval=500000000, n_intervals=0),
#        html.P("blah blah"),
#        html.H1("big text big textbig text big textbig text big text"),
#        html.Div([html.P(" "),],style={'height':'300'}),
    ],style={'width':'45%','display': 'inline-block'},),
    ], className='row'
)

default_text = """<p>I am getting unexpected results when adding items to sets that are in a list such that i can specify the set i need by indexing the list as shown in the following code:</p>
<pre><code>def get_friends_of_users(network):
f1 = lambda x: x[0]
f2 = lambda x: x[1]
users_friends = [set()] * max(max(network, key=f1)[0], max(network, key=f2)[1]+1)
net_users = set()
for i in network:
users_friends[i[0]].add(i[1])
users_friends[i[1]].add(i[0])
net_users.add(i[0])
net_users.add(i[1])
#print(users_friends)
return net_users, users_friends
</code></pre>

<p>let the variable network be the following list of tuples:</p>

<p><code>network = [(3, 5), (2, 1), (2, 4), (1, 5), (5, 0), (3, 2), (3, 0)]</code>
which describes users and their friends as (user_ID, friend_ID).</p>

<p>I want the variable <code>users_friends</code> to be a list of sets of all friends for each user as following:</p>

<p>User ID=0 has friends with IDs = 3, 5 </p>

<p>User ID=1 has friends with IDs = 2, 5 </p>

<p>User ID=2 has friends with IDs = 1, 3, 4 </p>

<p>User ID=3 has friends with IDs = 0, 2, 5 </p>

<p>User ID=4 has friends with IDs = 2 </p>

<p>User ID=5 has friends with IDs = 0, 1, 3.</p>

<p>But when i uncomment the print statement in the code i can see that every time i execute the statement <code>users_friends[i[0]].add(i[1])</code> or <code>users_friends[i[1]].add(i[0])</code> it adds the item to all sets in the list.</p>

<p>so, this was the output:</p>

<pre><code>[{3, 5}, {3, 5}, {3, 5}, {3, 5}, {3, 5}, {3, 5}]

[{1, 2, 3, 5}, {1, 2, 3, 5}, {1, 2, 3, 5}, {1, 2, 3, 5}, {1, 2, 3, 5}, {1, 2, 3, 5}]

[{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}]

[{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}]

[{0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}]

[{0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}]

[{0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5}]
</code></pre>

<p><strong><em>My question is: why it adds the item to all sets in the list and how to make it add to the set that i specify by indexing the list of those sets?</em></strong></p>

"""


@app.callback(
    Output("input_text", "value"),
    [Input("button-test", "n_clicks")],
    [State("link_text", "value")],
)
def input_test(n_clicks, link):
    """Populate the inputs with example text for the demo"""
    if n_clicks < 1:
        return ""
    if link != "":
        page = requests.get(link)
        tree = xhtml.fromstring(page.content)
        post = tree.xpath('//*[@id="question"]/div[2]/div[2]/div[1]')[0]
        #            print(post.text_content())
        return post.text_content()
    else:
        return default_text


@app.callback(
    Output("title_text", "value"),
    [Input("button-test", "n_clicks")],
    [State("link_text", "value")],
)
def title_test(n_clicks, link):
    "populate the title field"
    if n_clicks < 1:
        return ""
    if link != "":
        page = requests.get(link)
        tree = xhtml.fromstring(page.content)
        title = tree.xpath('//*[@id="question-header"]/h1/a/text()')[0]
        return title
    else:
        return "How can i add sets to a list in python?"


@app.callback(
    Output("tags_text", "value"),
    [Input("button-test", "n_clicks")],
    [State("link_text", "value")],
)
def input_test(n_clicks, link):
    """Populate the tags field"""
    if n_clicks  < 1:
        return ""
    if link != "":
        page = requests.get(link)
        tree = xhtml.fromstring(page.content)
        tags = tree.xpath('//*[@id="question"]/div[2]/div[2]/div[2]/div')[0]
        dirty_tags = tags.text_content().split(" ")
        clean_tags = []
        for t in dirty_tags:
            if t in ["", "\r\n"]:
                continue
            else:
                clean_tags.append(t)
        return ",".join(clean_tags)
    else:
        return "python, set, list"


@app.callback(
    dash.dependencies.Output("output", "children"),
    [dash.dependencies.Input("button", "n_clicks")],
    [State("input_text", "value"), State("tags_text", "value")],
)
def pipe(n_clicks, text, tags):
    """The main function, which parses the input fields: <text> and <tags>,
    packages them in json and sends them to Kafka for processing in
    the Spark Streaming application."""
    # on page load, all the buttons get clicked.
    if n_clicks < 1:
        return "Output will appear here"
    st = time.time()
    if tags.find(",") > 0:
        tags = tags.split(",")
    else:
        tags = [tags]

    # make sure Redis is clear before we submit
    redis_host = "10.0.0.10"
    redis_port = 6379
    r = redis.Redis(host=redis_host, port=redis_port)

    # connect to kafka cluster
    cluster = kafka.KafkaClient("10.0.0.11:9092")
    prod = kafka.SimpleProducer(cluster, async=False)

    # produce some messages
    ind = "".join(
        random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
        for _ in range(8)
    )

    # the database can sometimes have stale data
    for i in range(5):
        r.delete("success:{}|{}".format(ind, i))

    topic = "cloud"
    msg = {"text": text, "tags": tags, "index": ind}
    prod.send_messages(topic, json.dumps(msg).encode("utf-8"))

    success = None
    results = []

    # success flag signals that the spark streaming app has completed.
    while success == None:
        success = r.get("success:{}|4".format(ind))
        time.sleep(0.1)  # don't hammer the database

    # get top 5 values, stored in redis
    for i in range(5):
        results.append(
            r.get("success:{}|{}".format(ind, i)).split("|")
        )  # id, similarity
        r.delete("success:{}|{}".format(ind, i))

    # build output links
    output = []
    for res in results:
        title = r.hget(res[0][:-1], res[0][-1:] + ":t")
        ref = "https://www.stackoverflow.com/questions/{}/{}".format(
            res[0][3:], title.replace(" ", "-")
        )
        output.append([res[1], title, ref])

    return html.Div(
        [
            html.P("{} Seconds for round-trip calculation".format(time.time() - st)),
            html.Table(
                [html.Tr([html.Th(x) for x in ["Similarity", "title"]])]
                + [
                    html.Tr([html.Td(r[0]), html.A(r[1], href=r[2], target="_blank")])
                    for r in output
                ],
                style={"width": "100%"},
            ),
        ]
    )

#@app.callback(
#    Output("output", "children"),
#    [Input("button", "n_clicks")],
#    [State("input_text", "value"), State("tags_text", "value")],
#)
#def kafka_test(n_clicks, text, tags):
#    """The main function, which parses the input fields: <text> and <tags>,
#    packages them in json and sends them to Kafka for processing in
#    the Spark Streaming application."""
#    # on page load, all the buttons get clicked.
#    if n_clicks < 1:
#        return "Output will appear here"
#    st = time.time()
#    if tags.find(",") > 0:
#        tags = tags.split(",")
#    else:
#        tags = [tags]
#
#    # make sure Redis is clear before we submit
#    redis_host = "10.0.0.10"
#    redis_port = 6379
#    r = redis.Redis(host=redis_host, port=redis_port)
#
#    # connect to kafka cluster
#    cluster = kafka.KafkaClient("10.0.0.11:9092")
#    prod = kafka.SimpleProducer(cluster, async=False)


data = []


@app.callback(Output('update', 'interval'), [Input('kafka-test','n_clicks')])
def start_kafka(n_clicks):
    # set initial state to 'off', aka a long interval
    if n_clicks < 1:
	return 500000000
    return 500

@app.callback(Output('kafka', 'figure'), [Input('update', 'n_intervals')])
def gen_wind_speed(interval):
    # now = dt.datetime.now()
    # sec = now.second
    # minute = now.minute
    # hour = now.hour
    #
    # total_time = (hour * 3600) + (minute * 60) + (sec)
    global data
    for i in range(15):
        data.append(np.random.rand(1)[0])
    data = data[-200:]
    # con = sqlite3.connect("./Data/wind-data.db")
    # df = pd.read_sql_query('SELECT Speed, SpeedError, Direction from Wind where\
    #                         rowid > "{}" AND rowid <= "{}";'
    #                         .format(total_time-200, total_time), con)

    trace = Scatter(
        y=data,
        line=Line(
            color='#42C4F7'
        ),
        hoverinfo='skip',
        # error_y=ErrorY(
        #     type='data',
        #     array=df['SpeedError'],
        #     thickness=1.5,
        #     width=2,
        #     color='#B4E8FC'
        # ),
        mode='lines'
    )

    layout = Layout(
        height=450,
        xaxis=dict(
            range=[0, 200],
            showgrid=False,
            showline=False,
            zeroline=False,
            fixedrange=True,
            tickvals=[0, 50, 100, 150, 200],
            ticktext=['200', '150', '100', '50', '0'],
            title='Time Elapsed (sec)'
        ),
        yaxis=dict(
            range=[min(0, min(data)),
                   max(2,max(data))],
                   #max(45, max(data))],
            showline=False,
            fixedrange=True,
            zeroline=False,
            nticks=max(6, 12) #round(data/10))
        ),
        margin=Margin(
            t=45,
            l=50,
            r=50
        )
    )

    return Figure(data=[trace], layout=layout)



if __name__ == "__main__":
    app.run_server(debug=True, host="10.0.0.10", port="8889")
