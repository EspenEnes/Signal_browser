import dash
import dash_daq as daq
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from dash import dcc, no_update
from plotly_resampler import FigureResampler
from PySide6 import QtCore
from PySide6.QtCore import Property, QObject, Signal


class ThemeManager(QObject):
    def __init__(self):
        super().__init__()
        self._is_dark = False  # private attribute

    # Signal that will be emitted whenever is_dark changes
    is_dark_changed = Signal(bool)

    @Property(bool, notify=is_dark_changed)
    def is_dark(self):
        return self._is_dark

    @is_dark.setter
    def is_dark(self, value):
        if self._is_dark != value:
            self._is_dark = value
            self.is_dark_changed.emit(value)


theme_manager = ThemeManager()

is_dark = False


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
        self.fig = FigureResampler(go.Figure(), default_n_shown_samples=2500)
        self.fig.register_update_graph_callback(app=self._app, graph_id="fig", coarse_graph_id="trace-updater")
        self.theme_manager = theme_manager

        pio.templates["costum"] = go.layout.Template()
        pio.templates["costum"].layout.legend.update(dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ))
        # pio.templates["costum"].layout.hovermode = 'x unified'
        pio.templates["costum"].layout.margin = dict(l=10, r=10, b=10, t=100)

        self.fig.update_layout(template="plotly+costum")
        self._app.layout = self.root_layout({}, {})

        self.update_graph(self.fig)

    def root_layout(self, data, layout):
        base_layout = dash.html.Div(id="dark-theme-components-1", children=[
            dash.html.Div([dash.html.Div([dash.html.Button("Multiplot", id="multiplot-button", n_clicks=0),
                                          dcc.Checklist(
                                              id="toggle-cursor",
                                              options=[{"label": "cursor", "value": True}],
                                          )],
                                         style={"width": "100%", "display": "flex", "justify-content": "flex-start"}),
                           daq.ToggleSwitch(
                               id="toggle-theme",
                               label=["Light", "Dark"],
                               value=False)],
                          style={"width": "100%", "display": "flex", "justify-content": "space-between"}),
            dash.dcc.Graph(
                id="fig",
                style={ "margin": "0","padding": "0","height": "100%" },
                figure=dict(data=data, layout=layout),

                config={"scrollZoom": True}, ),
            dcc.Store(id="dark_memory", storage_type="session"),
            # , dcc.Interval(
            #     id='interval-component',
            #     interval=100,  # in milliseconds (1*1000 = 1 second)
            #     n_intervals=0
            # )
        ],
                                    style={"backgroundColor": "white", "color": "black",
                                           "outline": "8px solid rgb(255, 255, 255)", "height": "94vh", "width": "100%",
                                           "margin": "0",
                                           "padding": "0"})

        return base_layout

    # @dash.callback(dash.Output('fig', 'figure', allow_duplicate=True),
    #               dash.Input('interval-component', 'n_intervals'),
    #                dash.State('fig', 'figure'),
    #                prevent_initial_call=True
    #                )
    # def update_graph_live(n, fig):
    #
    #     return no_update

    def new_graph(self):
        self.fig.replace(go.Figure())
        self.update_graph(self.fig)

    def update_graph(self, fig):
        """Updates the graph with the given figure"""
        if self.theme_manager.is_dark:
            fig["layout"]["template"] = pio.templates["plotly_dark+costum"]
        else:
            fig["layout"]["template"] = pio.templates["plotly+costum"]
        self._app.layout = self.root_layout(fig.data, fig.layout)

    @dash.callback(
        dash.Output("fig", "figure", allow_duplicate=True),
        dash.Input("multiplot-button", "n_clicks"),
        dash.State("fig", "figure"),
        prevent_initial_call=True
    )
    def multiplot(n_clicks, fig):
        """multi-plot graph where each data series has its y-axis and corresponding color"""

        if fig and n_clicks:
            for ix, data in enumerate(fig["data"], start=0):
                if ix == 0:
                    color = fetch_color(data, ix)
                    data["yaxis"] = "y"
                    fig["layout"]["yaxis"] = dict(
                        showticklabels=False,
                        color=color,
                        tickformat=".3s"
                    )
                else:
                    color = fetch_color(data, ix)
                    data["yaxis"] = f"y{ix + 1}"
                    fig["layout"][f"yaxis{ix + 1}"] = dict(
                        color=color,
                        side="left",
                        anchor="free",
                        overlaying="y",
                        autoshift=True,
                        showgrid=False,
                        minor_showgrid=False,
                        tickformat=".3s"
                    )

            return fig

        return no_update

    @dash.callback(
        dash.Output("fig", "figure", allow_duplicate=True),
        dash.Input("toggle-cursor", "value"),
        dash.State("fig", "figure"),
        prevent_initial_call=True

    )
    def show_cursor(value, fig):

        if len(value) > 0:
            fig["layout"]["hovermode"] = "x unified"
            return fig
        fig["layout"]["hovermode"] = "closest"
        return fig

    @dash.callback(
        dash.Output("toggle-theme", "value"),
        dash.Input("dark-theme-components-1", "children")
    )
    def init_switch_bg(dark):
        if theme_manager.is_dark:
            return True
        else:
            return False

    @dash.callback(
        dash.Output("dark-theme-components-1", "style"),
        dash.Output("fig", "figure"),
        dash.Input("toggle-theme", "value"),
        dash.State("fig", "figure"),
        dash.State("dark-theme-components-1", "style"),
        prevent_initial_call=True
    )
    def switch_bg(dark, figure, currentStyle):
        global theme_manager

        if (dark):
            theme_manager.is_dark = True
            currentStyle.update(backgroundColor="black", color="white", outline="8px solid rgb(0, 0, 0)")
            figure["layout"]["template"] = pio.templates["plotly_dark+costum"]

        else:
            theme_manager.is_dark = False
            currentStyle.update(backgroundColor="white", color="black", outline="8px solid rgb(255, 255, 255)")
            figure["layout"]["template"] = pio.templates["plotly+costum"]
        return currentStyle, figure

    def run(self):
        """Runs the app"""
        self._app.run(host=self.host, port=self.port, debug=False, use_reloader=False)

    def stop(self):
        self.terminate()
