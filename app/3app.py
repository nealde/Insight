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


external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
#external_stylesheets=[""]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
#app.css.append_css({'external_url': ( 
#    'cdn.jsdelivr.net/gh/lwileczek/Dash@master/v5.css'
#)}) 
app.css.append_css({'external_url': ( ' rawgit.com/lwileczek/Dash/master/undo_redo5.css')})

app.layout = html.Div(
    [
        html.Div(
            className="app-header",
            children=[
                html.H1("Stack'D"),
                html.H3(
                    "Check to see if your question has already been answered below!"
                ),
                html.H5("Or, enter a Stack Overflow link below to find related posts:"),
                html.Div([html.Button("Randomize!",id='randomize-button'),],style={'width':'80%'}),
                dcc.Textarea(
                    placeholder="Enter a Stack Overflow link...",
                    value="",
                    style={"width": "80%", "height": "15"},
                    id="link_text",
                ),
                html.H5("Title:"),
                dcc.Textarea(
                    placeholder="Enter a title...",
                    value="",
                    style={"width": "80%", "height": "15"},
                    id="title_text",
                ),
            ],
        ),
        html.H5("Question Body:"),
        dcc.Textarea(
            placeholder="Enter a question...",
            value="",
            style={"width": "80%", "height": "200"},
            id="input_text",
        ),
        html.H5("Enter tags to filter by below:"),
        dcc.Textarea(
            placeholder="Enter some tags...",
            value="",
            style={"width": "80%", "height": "15"},
            id="tags_text",
        ),
        html.Div([
        html.Button("Submit", id="button"),
        html.Button("Scrape", id="button-test"),
        ],style={'width':'80%'}),
        html.Div(["Test output will be reported here"], id="output"),
    ]
)


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


@app.callback(Output('link_text','value'),
              [Input('randomize-button','n_clicks'),])
def get_random_link(n_clicks):
    if n_clicks < 1:
        return ""
    # get the newest stack overflow questions
#    page = requests.get('https://stackoverflow.com/questions?sort=newest')
#    tree = xhtml.fromstring(page.content)
#    questions = tree.xpath('//*[@id="questions"]')  
#    ref = random.choice(questions[0])
#    full_ref = 'https://stackoverflow.com/{}'.format(ref[1][0][0].values()[0])
    full_ref = "https://stackoverflow.com/questions/2213923/removing-duplicates-from-a-list-of-lists"
    return full_ref


@app.callback(
    dash.dependencies.Output("output", "children"),
    [dash.dependencies.Input("button", "n_clicks")],
    [State("input_text", "value"), State("tags_text", "value")],
)
def kafka_test(n_clicks, text, tags):
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
            html.P("{:04.2f} Seconds for round-trip calculation".format(time.time() - st)),
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


if __name__ == "__main__":
    app.run_server(host="10.0.0.10", port="8889")
