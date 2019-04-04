import dash
import dash_core_components as dcc
import dash_html_components as html
print(dcc.__version__)


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.Div(
        className="app-header",
        children=[
            html.Div('Cloud Compare', className="app-header--title")
        ]
    ),
    dcc.Textarea(
    placeholder='Enter a value...',
    value='',
    style={'width': '100%'},
    id='input_text'
),
html.Button('Submit', id='button'),
html.Div(id='output-container-button',
             children='Output will be reported here')
])


@app.callback(
    dash.dependencies.Output('output-container-button', 'children'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('input_text', 'value')])
def update_output(n_clicks, value):
    # redis_host = '10.0.0.7'
    # redis_port = 6379
    # redis_password = 'AhrIykRVjO9GHA52kmYou7iUrsDbzJL+/7vjeTYhsLmpskyAY8tnucf4QJ7FpvVzFNNKuIZVVkh1LRxF'
    # r = redis.Redis(host=redis_host, port=redis_port, password=redis_pasword)
    # response = len(r.get(input))
    # print(response)
    return 'The response is "{}" characters long and the button has been clicked {} times'.format(
        response,
        n_clicks
    )


if __name__ == '__main__':
    app.run_server(debug=True)
