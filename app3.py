import dash
from dash.dependencies import Output, Input, State
import dash_core_components as dcc
import dash_html_components as html
import redis
import time
import kafka
import json
import random
import requests
#import lxml
from lxml import html as xhtml
#print(dir(lxml))

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.Div(
        className="app-header",
        children=[
            html.H1('Cloud Compare'),
            html.H3("An app for text comparison at cloud-scale.  Please submit a question below to test it against the previously recorded Stack Overflow questions!"),
        html.H5("Enter a Stack Overflow link below to find related posts!"),
        dcc.Textarea(placeholder='Enter a Stack Overflow link...',
            value='',
            style={'width':'100%','height':'15'},
            id='link_text'),
#        html.Button("Scrape!",id='scrape_button'),
        html.H5("Title"),
        dcc.Textarea(placeholder='Enter a title...',
            value='',
            style={'width':'100%','height':'15'},
            id='title_text'),]
    ),
    html.H5("Question Body:"),
    dcc.Textarea(
        placeholder='Enter a question!',
        value='',
        style={'width': '100%', 'height':'200'},
        id='input_text'
),
html.H5('Enter tags to filter by below'),
#html.A('link to external site', href='https://plot.ly'),
dcc.Textarea(
    placeholder='Enter some tags...',
    value='',
    style={'width':'100%', 'height':'15'},
    id='tags_text'
),

html.Button('Submit', id='button'),
html.Button('Scrape', id='button-test'),
#html.Button('clear output', id='button-clear'),
html.Div(['Test output will be reported here'], id='output'),
])

# question: https://stackoverflow.com/questions/55621027/how-can-i-add-items-to-sets-in-a-list-in-python


@app.callback(
    Output('input_text','value'),
    [Input('button-test','n_clicks')],
    [State('link_text','value')],)
def input_test(n_clicks, link):
    """Populate the inputs with example text for the demo"""
#    print(link)

    if n_clicks > 0:
        if link != "":
            page = requests.get(link)
            tree = xhtml.fromstring(page.content)
            post = tree.xpath('//*[@id="question"]/div[2]/div[2]/div[1]')[0]
#            print(post.text_content())
            return post.text_content()
        else:
            return """<p>I am getting unexpected results when adding items to sets that are in a list such that i can specify the set i need by indexing the list as shown in the following code:</p>

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
    Output('title_text','value'),
    [Input('button-test','n_clicks')],
    [State('link_text','value')],)
def title_test(n_clicks, link):
    "populate the title field"
    if n_clicks > 0:
        if link != "":
            page = requests.get(link)
            tree = xhtml.fromstring(page.content)
            title = tree.xpath('//*[@id="question-header"]/h1/a/text()')[0]
            return title
        else:
            return "How can i add sets to a list in python?"

@app.callback(
    Output('tags_text','value'),
    [Input('button-test','n_clicks')],
    [State('link_text','value')],)
def input_test(n_clicks, link):
    """Populate the tags field"""
    if n_clicks > 0:
        if link != "":
            page = requests.get(link)
            tree = xhtml.fromstring(page.content)
            tags = tree.xpath('//*[@id="question"]/div[2]/div[2]/div[2]/div')[0]
            dirty_tags = tags.text_content().split(' ')
            clean_tags = []
            for t in dirty_tags:
                if t in ['','\r\n']:
                    continue
                else:
                    clean_tags.append(t)
            return ','.join(clean_tags)
        else:
            return "python, set, list"



@app.callback(
    dash.dependencies.Output('output', 'children'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [State('input_text','value'),State('tags_text','value')])
def kafka_test(n_clicks, text, tags):
    """The main function, which parses the input fields: <text> and <tags>,
    packages them in json and sends them to Kafka for processing in
    the Spark Streaming application."""
    if n_clicks>0:
        st = time.time()
        if tags.find(',')>0:
            tags = tags.split(',')
        else:
            tags = [tags]

        # make sure Redis is clear before we submit
        redis_host = '10.0.0.10'
        redis_port = 6379
        r = redis.Redis(host=redis_host, port=redis_port)

        for i in range(5):
#            results.append(r.get('success:0|'+str(i)).split('|')) # id, similarity
            r.delete('success:0|'+str(i))


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
        success = None
        results = []

        # success flag signals that the spark streaming app has completed.
        while success == None:
            success = r.get('success:0|4')
            time.sleep(.1)
        print(success)
        for i in range(5):
            results.append(r.get('success:0|'+str(i)).split('|')) # id, similarity
            r.delete('success:0|'+str(i))
        print(results)
        output = []
        for res in results:
             title = r.hget(res[0][:-1], res[0][-1:]+":t")
             ref = "https://www.stackoverflow.com/questions/{}/{}"\
             .format(res[0][3:],title.replace(' ','-'))
             output.append([res[1],title,ref])

        print output
    if n_clicks > 0:
        return html.Div([html.P('{} Seconds for round-trip calculation'.format(time.time()-st)),
                html.Table(
                [html.Tr([html.Th(x) for x in ['Similarity', 'title']])]+
                [html.Tr([html.Td(r[0]), html.A(r[1], href=r[2], target="_blank")]) for r in output], style={'width':'100%'}),])
                #[html.Tr([*[html.Td(o) for o in r]) for r in output[:-1],html.A(output[-1])]], style={'width':"100%"})
    else:
        return "Output will appear here"


if __name__ == '__main__':
    app.run_server(debug=True, host="10.0.0.10", port="8889")
