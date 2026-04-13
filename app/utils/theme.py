"""MLB Toolbox — Plotly theme helper."""


def plotly_theme(**overrides) -> dict:
    """Return a base Plotly layout dict — dark slate + deep blue theme.

    Pass keyword overrides to customise per-chart (e.g. title, height, showlegend).
    Nested dict overrides are shallow-merged with the base dicts.
    """
    base: dict = dict(
        paper_bgcolor="#141d2e",   # match main bg
        plot_bgcolor="#1c2a42",    # slightly lifted card surface
        font=dict(color="#7a9ebc", size=11),
        title=dict(font=dict(color="#d6e8f8", size=13), x=0.02),
        xaxis=dict(
            gridcolor="#1e3250", linecolor="#1e3250",
            zerolinecolor="#253d58", zerolinewidth=1,
            tickfont=dict(color="#7a9ebc"), title_font=dict(color="#a8c8e8"),
        ),
        yaxis=dict(
            gridcolor="#1e3250", linecolor="#1e3250",
            zerolinecolor="#253d58", zerolinewidth=1,
            tickfont=dict(color="#7a9ebc"), title_font=dict(color="#a8c8e8"),
        ),
        legend=dict(
            bgcolor="#1c2a42", bordercolor="#253d58", borderwidth=1,
            font=dict(color="#7a9ebc"),
        ),
        margin=dict(l=50, r=20, t=45, b=50),
        showlegend=False,
        transition=dict(duration=400, easing="cubic-in-out"),
    )
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            base[k] = {**base[k], **v}
        else:
            base[k] = v
    return base
