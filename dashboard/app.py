
import dash
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

engine = create_engine("postgresql://bankuser:bankpass@localhost:5432/basel3")

df = pd.read_sql("select * from regulatory_metrics", engine)

fig = px.bar(df, x="bank_id", y="car")

app = dash.Dash(__name__)

app.layout = dash.html.Div([
    dash.dcc.Graph(figure=fig)
])

if __name__ == "__main__":
    app.run(debug=True)
