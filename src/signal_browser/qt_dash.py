import dash
from PySide6 import QtCore
import plotly.graph_objects as go


class DashThread(QtCore.QThread):
    """A thread that runs a dash app"""
    def __init__(self, parent=None):
        """Initializes the thread"""
        super().__init__(parent)
        self._app = dash.Dash()
        self._app.layout = dash.html.Div()
        fig = go.Figure()
        fig.update_xaxes(minor_showgrid=True, gridwidth=1, gridcolor='gray',  minor_griddash="dot")
        fig.update_yaxes(minor_showgrid=True, gridwidth=1, gridcolor='gray', minor_griddash="dot")
        self.update_graph(fig)

    def update_graph(self, fig):
        """Updates the graph with the given figure"""
        self._app.layout = dash.html.Div(children=[
            dash.dcc.Graph(
                id='fig',
                figure={
                    'data': fig.data,
                    'layout': fig.layout
                },  style={'height': '100vh'}),
        ], style={'height': '100vh'})

    def run(self):
        """Runs the app"""
        self._app.run_server()