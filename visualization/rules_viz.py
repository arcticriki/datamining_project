import matplotlib.pyplot as plt
import pandas as pd
import plotly as py
import plotly.plotly as pyp
import plotly.graph_objs as go
from plotly.graph_objs import *
import random

timestamp = "20170604-105304"
file_name = "results/{}/rules".format(timestamp)

df = pd.read_csv(file_name, sep=';')

print df.describe()

df = df[df.lift > 1]
num_samples = 4000
#sampling
if len(df.index)>num_samples :
    df = df.sample(num_samples)


df.columns =['rule','support','confidence','lift','conviction']
rules = list(df.rule)
confidence = list(df.confidence)
support = list(df.support)
lift = list(df.lift)
conviction = list(df.conviction)


trace1 = go.Scatter(
    y = confidence,
    x = support,
    mode='markers',
    text = rules,
    marker=dict(
        size='16',
        color = lift,#set color equal to a variable
        colorscale ='Portland',
        showscale=True
    )
)

data = [trace1]

pyp.plot(data, filename=timestamp)


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
