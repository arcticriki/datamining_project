import matplotlib.pyplot as plt
import pandas as pd
import plotly as py
import plotly.plotly as pyp
import plotly.graph_objs as go
from plotly.graph_objs import *
from plotly import tools
import random



col = "Sex"
subgroups = ['M', 'F']

def import_data(col, val):

    file_name = "results/{}:{}/rules".format(col,val)

    df = pd.read_csv(file_name, sep=';')

    print df.describe()

    df = df[(df.lift > 2) & (df.lift < 20)]
    num_samples = 4000
    #sampling
    if len(df.index)>num_samples :
    	df = df.sample(num_samples)

    data = {}

    df.columns =['rule','support','confidence','lift','conviction']
    data['rules'] = list(df.rule)
    data['confidence'] = list(df.confidence)
    data['support'] = list(df.support)
    data['lift'] = list(df.lift)
    data['conviction'] = list(df.conviction)

    return data


traces = []

for subgroup in subgroups :
    
    data = import_data(col, subgroup)

    traces.append( go.Scatter(
            y = data['confidence'],
            x = data['support'],
            mode='markers',
            text = data['rules'],
            marker=dict(
                size='16',
                color = data['lift'],#set color equal to a variable
                colorscale ='Portland',
                showscale=False,
            ),
            name=subgroup
        )
    )

layout = go.Layout(
    xaxis=dict(
        title='Support',
        titlefont=dict(
            family='Courier New, monospace',
            size=18,
            color='#7f7f7f'
        )
    ),
    yaxis=dict(
        title='Confidence',
        titlefont=dict(
            family='Courier New, monospace',
            size=18,
            color='#7f7f7f'
        )
    )
)

fig = tools.make_subplots(rows=1, cols=2,subplot_titles=(subgroups))
fig['layout'].update(showlegend=False, title=col)

fig.append_trace(traces[0], 1, 1)
fig.append_trace(traces[1], 1, 2)

# data = [trace1]
# fig = go.Figure(data=data, layout=layout)

pyp.plot(fig, filename=col)


#Possible colors for the scatter plot
# ['Blackbody',
# 'Bluered',
# 'Blues',
# 'Earth',
# 'Electric',
# 'Greens',
# 'Greys',
# 'Hot',
# 'Jet',
# 'Picnic',
# 'Portland',
# 'Rainbow',
# 'RdBu',
# 'Reds',
# 'Viridis',
# 'YlGnBu',
# 'YlOrRd']
