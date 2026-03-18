import dash
from dash import dcc, html
import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import create_engine

# ── Database connection ──────────────────────────────────────────────────────
engine = create_engine("postgresql://bankuser:bankpass@localhost:5432/basel3")
df = pd.read_sql("select * from regulatory_metrics", engine)

# ── Colours ──────────────────────────────────────────────────────────────────
PASS_COLOR  = "#2ECC71"   # green
FAIL_COLOR  = "#E74C3C"   # red
BG          = "#0D1117"   # near-black background
CARD_BG     = "#161B22"   # card background
BORDER      = "#30363D"   # subtle border
TEXT        = "#E6EDF3"   # primary text
SUBTEXT     = "#8B949E"   # secondary text
ACCENT      = "#58A6FF"   # blue accent

BENCHMARKS = {"car": 14.0, "lcr": 120.0, "npl_ratio": 1.5}
LABELS     = {"car": "CAR %", "lcr": "LCR %", "npl_ratio": "NPL %"}
TITLES     = {
    "car":       "Capital Adequacy Ratio (CAR)",
    "lcr":       "Liquidity Coverage Ratio (LCR)",
    "npl_ratio": "Non-Performing Loans (NPL)",
}
DESCRIPTIONS = {
    "car":       "Minimum 14% — Measures if the bank has enough own capital to cover risky loans.",
    "lcr":       "Minimum 120% — Measures if the bank has enough cash to survive the next 30 days.",
    "npl_ratio": "Maximum 1.5% — Measures how many borrowers are NOT paying back their loans.",
}

# ── Helper: build one bar chart ───────────────────────────────────────────────
def make_chart(metric):
    benchmark = BENCHMARKS[metric]
    higher_is_better = metric != "npl_ratio"

    colors = []
    for val in df[metric]:
        if higher_is_better:
            colors.append(PASS_COLOR if val >= benchmark else FAIL_COLOR)
        else:
            colors.append(PASS_COLOR if val <= benchmark else FAIL_COLOR)

    fig = go.Figure()

    # Bars
    fig.add_trace(go.Bar(
        x=df["bank_id"],
        y=df[metric],
        marker_color=colors,
        marker_line_width=0,
        text=[f"{v:.1f}%" for v in df[metric]],
        textposition="outside",
        textfont=dict(color=TEXT, size=13, family="monospace"),
        hovertemplate="<b>%{x}</b><br>" + LABELS[metric] + ": %{y:.2f}%<extra></extra>",
    ))

    # Benchmark line
    fig.add_hline(
        y=benchmark,
        line_dash="dash",
        line_color=ACCENT,
        line_width=2,
        annotation_text=f"Benchmark: {benchmark}%",
        annotation_font_color=ACCENT,
        annotation_font_size=11,
        annotation_position="top right",
    )

    fig.update_layout(
        plot_bgcolor=CARD_BG,
        paper_bgcolor=CARD_BG,
        font_color=TEXT,
        font_family="monospace",
        margin=dict(t=20, b=40, l=40, r=20),
        xaxis=dict(
            showgrid=False,
            tickfont=dict(size=13, color=TEXT),
            linecolor=BORDER,
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor=BORDER,
            tickfont=dict(size=11, color=SUBTEXT),
            ticksuffix="%",
            linecolor=BORDER,
        ),
        hoverlabel=dict(bgcolor=CARD_BG, font_color=TEXT, bordercolor=BORDER),
        bargap=0.35,
    )
    return fig


# ── Helper: summary stat cards ────────────────────────────────────────────────
def stat_card(title, value, sub, color):
    return html.Div([
        html.P(title, style={"color": SUBTEXT, "fontSize": "11px", "margin": "0 0 4px 0",
                              "textTransform": "uppercase", "letterSpacing": "1px"}),
        html.H2(value, style={"color": color, "margin": "0 0 2px 0", "fontSize": "28px",
                               "fontFamily": "monospace"}),
        html.P(sub, style={"color": SUBTEXT, "fontSize": "11px", "margin": "0"}),
    ], style={
        "background": CARD_BG,
        "border": f"1px solid {BORDER}",
        "borderRadius": "8px",
        "padding": "16px 20px",
        "flex": "1",
        "minWidth": "140px",
    })


# ── Pass / fail summary ───────────────────────────────────────────────────────
def pass_fail_row():
    rows = []
    for _, row in df.iterrows():
        cells = [html.Td(row["bank_id"], style={"padding": "10px 16px", "color": TEXT,
                                                 "fontWeight": "bold", "fontFamily": "monospace"})]
        for metric, bm in BENCHMARKS.items():
            val = row[metric]
            higher_is_better = metric != "npl_ratio"
            passed = val >= bm if higher_is_better else val <= bm
            badge_color = PASS_COLOR if passed else FAIL_COLOR
            label = "PASS" if passed else "FAIL"
            cells.append(html.Td(
                html.Span(f"{val:.1f}%  {label}", style={
                    "background": badge_color + "22",
                    "color": badge_color,
                    "border": f"1px solid {badge_color}",
                    "borderRadius": "4px",
                    "padding": "3px 10px",
                    "fontSize": "12px",
                    "fontFamily": "monospace",
                }),
                style={"padding": "8px 16px"}
            ))
        rows.append(html.Tr(cells, style={"borderBottom": f"1px solid {BORDER}"}))
    return rows


# ── Count overall stats ───────────────────────────────────────────────────────
total_banks   = len(df)
car_pass      = sum(1 for v in df["car"]       if v >= 14.0)
lcr_pass      = sum(1 for v in df["lcr"]       if v >= 120.0)
npl_pass      = sum(1 for v in df["npl_ratio"] if v <= 1.5)
total_checks  = total_banks * 3
total_pass    = car_pass + lcr_pass + npl_pass
compliance_pct = round(total_pass / total_checks * 100)

# ── App layout ────────────────────────────────────────────────────────────────
app = dash.Dash(__name__)
app.title = "Basel III Regulatory Dashboard"

app.layout = html.Div(style={"background": BG, "minHeight": "100vh", "padding": "32px",
                               "fontFamily": "monospace"}, children=[

    # ── Header ──
    html.Div([
        html.Div([
            html.H1("Basel III Regulatory Dashboard",
                    style={"color": TEXT, "margin": "0", "fontSize": "24px", "fontWeight": "bold"}),
            html.P("Automated compliance monitoring · March 2026",
                   style={"color": SUBTEXT, "margin": "4px 0 0 0", "fontSize": "13px"}),
        ]),
        html.Div([
            html.Span("● LIVE", style={"color": PASS_COLOR, "fontSize": "12px",
                                        "background": PASS_COLOR + "22",
                                        "border": f"1px solid {PASS_COLOR}",
                                        "borderRadius": "4px", "padding": "4px 10px"})
        ])
    ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
               "marginBottom": "28px"}),

    # ── Stat cards ──
    html.Div([
        stat_card("Total Banks",      str(total_banks),          "monitored",              ACCENT),
        stat_card("CAR Compliance",   f"{car_pass}/{total_banks}", f"above 14% threshold",  PASS_COLOR if car_pass == total_banks else FAIL_COLOR),
        stat_card("LCR Compliance",   f"{lcr_pass}/{total_banks}", f"above 120% threshold", PASS_COLOR if lcr_pass == total_banks else FAIL_COLOR),
        stat_card("NPL Compliance",   f"{npl_pass}/{total_banks}", f"below 1.5% threshold", PASS_COLOR if npl_pass == total_banks else FAIL_COLOR),
        stat_card("Overall Score",    f"{compliance_pct}%",       f"{total_pass}/{total_checks} checks passed",
                  PASS_COLOR if compliance_pct >= 80 else FAIL_COLOR),
    ], style={"display": "flex", "gap": "12px", "marginBottom": "28px", "flexWrap": "wrap"}),

    # ── 3 Charts ──
    html.Div([
        html.Div([
            html.Div([
                html.H3(TITLES[metric], style={"color": TEXT, "margin": "0 0 4px 0", "fontSize": "14px"}),
                html.P(DESCRIPTIONS[metric], style={"color": SUBTEXT, "margin": "0 0 12px 0", "fontSize": "11px"}),
            ]),
            dcc.Graph(figure=make_chart(metric), config={"displayModeBar": False},
                      style={"height": "280px"}),
        ], style={
            "background": CARD_BG,
            "border": f"1px solid {BORDER}",
            "borderRadius": "8px",
            "padding": "20px",
            "flex": "1",
            "minWidth": "280px",
        })
        for metric in ["car", "lcr", "npl_ratio"]
    ], style={"display": "flex", "gap": "16px", "marginBottom": "28px", "flexWrap": "wrap"}),

    # ── Pass / Fail Table ──
    html.Div([
        html.H3("Bank Compliance Summary", style={"color": TEXT, "margin": "0 0 16px 0", "fontSize": "14px"}),
        html.Table(
            [html.Thead(html.Tr([
                html.Th("Bank",      style={"padding": "10px 16px", "color": SUBTEXT, "textAlign": "left",
                                            "borderBottom": f"1px solid {BORDER}", "fontSize": "11px",
                                            "textTransform": "uppercase", "letterSpacing": "1px"}),
                html.Th("CAR",       style={"padding": "10px 16px", "color": SUBTEXT, "textAlign": "left",
                                            "borderBottom": f"1px solid {BORDER}", "fontSize": "11px",
                                            "textTransform": "uppercase", "letterSpacing": "1px"}),
                html.Th("LCR",       style={"padding": "10px 16px", "color": SUBTEXT, "textAlign": "left",
                                            "borderBottom": f"1px solid {BORDER}", "fontSize": "11px",
                                            "textTransform": "uppercase", "letterSpacing": "1px"}),
                html.Th("NPL",       style={"padding": "10px 16px", "color": SUBTEXT, "textAlign": "left",
                                            "borderBottom": f"1px solid {BORDER}", "fontSize": "11px",
                                            "textTransform": "uppercase", "letterSpacing": "1px"}),
            ]))] +
            [html.Tbody(pass_fail_row())],
            style={"width": "100%", "borderCollapse": "collapse"}
        ),
    ], style={
        "background": CARD_BG,
        "border": f"1px solid {BORDER}",
        "borderRadius": "8px",
        "padding": "20px",
    }),

])

if __name__ == "__main__":
    app.run(debug=True)
