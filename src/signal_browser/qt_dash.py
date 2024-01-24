import dash
from PySide6 import QtCore
import plotly.graph_objects as go
import plotly.express as px
from dash import no_update
from plotly_resampler import FigureResampler


def fetch_color(data, index):
    colors = px.colors.qualitative.Plotly
    if "line" in data and data["line"]["color"]:
        return data["line"]["color"]
    return colors[index - len(colors) * (index // len(colors))]


class DashThread(QtCore.QThread):
    """A thread that runs a dash app"""

    def __init__(self, parent=None, host="http://127.0.0.1", port=8050):
        """Initializes the thread"""
        super().__init__(parent)
        self.host = host
        self.port = port
        self._app = dash.Dash()
        self._app.layout = dash.html.Div()
        self.fig = FigureResampler(go.Figure(), default_n_shown_samples=2500)
        self.fig.update_xaxes(minor_showgrid=True, gridwidth=1, gridcolor='lightgray', minor_griddash="dot")
        self.fig.update_yaxes(minor_showgrid=True, gridwidth=1, gridcolor='lightgray', minor_griddash="dot")
        self.fig.update_layout(legend=dict(
            orientation="h",
            yanchor="bottom",
            xanchor="right",
            x=1
        ))

        self.fig.register_update_graph_callback(app=self._app, graph_id="fig", coarse_graph_id="trace-updater")

        self.update_graph(self.fig)

    def update_progress(self, progress_fig):
        """Updates the graph with the given figure"""
        self._app.layout = dash.html.Div(
            children=[
                dash.html.Button('Multiplot', id='multiplot-button', n_clicks=0),
                dash.dcc.Graph(
                    id='fig',
                    figure={'data': progress_fig.data, 'layout': progress_fig.layout},
                    style={'height': '100vh'},
                    config={"scrollZoom": True},
                ),
            ],
            style={'height': '100vh'},
        )

    def update_graph(self, fig):
        """Updates the graph with the given figure"""
        self._app.layout = dash.html.Div(
            children=[
                dash.html.Button('Multiplot', id='multiplot-button', n_clicks=0),
                dash.dcc.Graph(
                    id='fig',
                    figure={'data': fig.data, 'layout': fig.layout},
                    style={'height': '100vh'},
                    config={"scrollZoom": True},
                ),
            ],
            style={'height': '100vh'},
        )

    @dash.callback(
        dash.Output('fig', 'figure'), dash.Input('multiplot-button', 'n_clicks'), dash.State('fig', 'figure')
    )
    def multiplot(n_clicks, fig):
        """multi-plot graph where each data series has its y-axis and corresponding color"""
        if fig and n_clicks:
            for ix, data in enumerate(fig['data']):
                if ix == 0:
                    color = fetch_color(data, ix)
                    data['yaxis'] = 'y'
                    fig['layout'][f'yaxis'] = dict(
                        color=color,
                        tickformat='.3s'
                    )
                else:
                    color = fetch_color(data, ix)
                    data['yaxis'] = f'y{ix + 1}'
                    fig['layout'][f'yaxis{ix + 1}'] = dict(
                        color=color,
                        side='left',
                        anchor="free",
                        overlaying='y',
                        autoshift=True,
                        showgrid=False,
                        minor_showgrid=False,
                        tickformat='.3s'
                    )

            return fig

        return no_update

    def run(self):
        """Runs the app"""
        self._app.run(host=self.host, port=self.port, debug=False, use_reloader=False)

    def stop(self):
        self.terminate()
