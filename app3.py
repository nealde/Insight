import dash
from dash.dependencies import Output, Input, State
import dash_core_components as dcc
import dash_html_components as html
import redis
import time
import kafka
import json
import random



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.Div(
        className="app-header",
        children=[
            html.H1('Cloud Compare'),
            html.H3("An app for text comparison at cloud-scale.  Please submit a question below to test it against the previously recorded Stack Overflow questions!"),
        ]
    ),
    dcc.Textarea(
    placeholder='Enter a value...',
    value='',
    style={'width': '100%', 'height':'300'},
    id='input_text'
),
html.H5('Enter tags to filter by below'),
dcc.Textarea(
    placeholder='Enter some tags...',
    value='',
    style={'width':'100%', 'height':'30'},
    id='tags_text'
),

html.Button('Submit', id='button'),
html.Button('test', id='button-test'),
html.Button('clear output', id='button-clear'),
html.Div(['Test output will be reported here'], id='output'),
])


@app.callback(
    Output('input_text','value'),
    [Input('button-test','n_clicks')],)
def input_test(n_clicks):
    """Populate the inputs with example text for the demo"""
    if n_clicks > 0:
        return """<p> it seems openstreetmap has changed their licensing scheme as a result lots of data were deleted as shown in the attached picture which is grafton nsw 2460 australia almost all streets are gone.</p>   <p> my question is: is there any way to download the old data somewhere by providing lat/lng's? (i understand that there could be some old archives for world or some countries but that doesn't work for me because at the moment my application is not capable to process those massive data files)</p>   <p> if there's no way to download the old data is there any other good free map data (not images) available?</p>   <p> also i've noticed that there're 4 options at the top right corner the other three except standard seem to be showing all streets. they are (at least mapquest) based on osm data but not the one we get from the "export" section of openstreetmap.org is that correct? </p>   <p> edit: oops as a new user i cannot post images.. the below link may work (or may not):</p>   <p> <a href="http://i.stack.imgur.com/ieixt.png" rel="nofollow"> http://i.stack.imgur.com/ieixt.png</a> </p>   <p> (it's just 2 snapshots of grafton nsw 2460 australia one of standard one of mapquest open)</p>
"""

@app.callback(
    Output('tags_text','value'),
    [Input('button-test','n_clicks')],)
def input_test(n_clicks):
    """Populate the tags field"""
    if n_clicks > 0:
        return "openstreetmap"



@app.callback(
    dash.dependencies.Output('output', 'children'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [State('input_text','value'),State('tags_text','value')])
def kafka_test(n_clicks, text, tags):
    """The main function, which parses the input fields: <text> and <tags>,
    packages them in json and sends them to Kafka for processing in
    the Spark Streaming application."""
    if n_clicks>0:
        if tags.find(',')>0:
            tags = tags.split(',')
        else:
            tags = [tags]

        # connect to kafka cluster
        cluster= kafka.KafkaClient("10.0.0.11:9092")
        prod = kafka.SimpleProducer(cluster, async=False)

        # produce some messages
        topic="cloud"
        msg_list = []
        for i in range(1):
            data = {'text': text,
                    'tags':tags,
                    'index':i}
            msg_list.append(data)
        if n_clicks != 0:
            prod.send_messages(topic, *[json.dumps(d).encode('utf-8') for d in msg_list])
        redis_host = '10.0.0.10'
        redis_port = 6379
        r = redis.Redis(host=redis_host, port=redis_port)
        success = None
        results = []
        # success flag signals that the spark streaming app has completed.
        while success == None:
            success = r.get('success:0|4')
        for i in range(5):
            results.append(r.get('success:0|'+str(i)).split('|')) # id, similarity
            r.delete('success:0|'+str(i))
        print(results)
        output = []
        for res in results:
             title = r.get(res[0]).split('|')[0]
             ref = "<a ref='www.stackoverflow.com/questions/{}/{}'> {} </a>"\
             .format(res[0][3:],title.replace(' ','-'),title)
             output.append([res[1],title , ref])

        print output
    if n_clicks > 0:
        return html.Table(
                [html.Tr([html.Th(x) for x in ['Similarity', 'title', 'Hyperlink']])]+
                [html.Tr([html.Td(o) for o in r]) for r in output], style={'width':"100%"})
    else:
        return "Output will appear here"


if __name__ == '__main__':
    app.run_server(debug=True, host="10.0.0.10", port="8889")
