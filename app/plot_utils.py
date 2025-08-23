from typing import List, Optional, Tuple

import numpy as np
import numpy.typing as npt
import plotly.graph_objects as go
import polars as pl
from scipy.interpolate import CubicSpline


def plot_yield_curve_spline(
    maturities: npt.NDArray[np.float64],
    yields: npt.NDArray[np.float64],
    title: Optional[str] = None,
    subtitle: Optional[str] = None,
    n_points: int = 1000,
    marker_size: int = 8,
    line_width: int = 2,
    show_legend: bool = True,
    y_buffer: float = 0.1,
    y_tick_interval: float = 0.1,
) -> go.Figure:
    """
    Build an interactive yield curve chart with cubic spline interpolation.

    Parameters
    ----------
    maturities : npt.NDArray[np.float64]
        Array of maturity values (in weeks).
    yields : npt.NDArray[np.float64]
        Array of yield values in percentages (not decimal).
    title : Optional[str]
        Main figure title.
    subtitle : Optional[str]
        Subtitle shown under the main title.
    n_points : int
        Number of interpolation points for the spline curve, defaults to 1000.
    marker_size : int
        Size of the markers for raw data points, defaults to 8.
    line_width : int
        Line width for the spline fit, defaults to 2.
    show_legend : bool
        Whether to show the legend, defaults to True.
    y_buffer : float
        Extra buffer above and below the y-axis limits (in percentage points).
    y_tick_interval : float
        Spacing between y-axis ticks (in percentage points).

    Returns
    -------
    fig : plotly.graph_objects.Figure
        Plotly figure.
    """
    rates: npt.NDArray[np.float64] = yields / 100.0
    # Fit cubic spline
    spline: CubicSpline = CubicSpline(maturities, rates)
    xs: npt.NDArray[np.float64] = np.linspace(
        float(maturities.min()), float(maturities.max()), n_points
    )
    ys: npt.NDArray[np.float64] = spline(xs)

    # Compute y-axis range with buffer
    min_y: float = min((rates.min() * 100), (ys.min() * 100)) - y_buffer
    max_y: float = max((rates.max() * 100), (ys.max() * 100)) + y_buffer

    # Round to nearest tick interval
    min_y = np.floor(min_y / y_tick_interval) * y_tick_interval
    max_y = np.ceil(max_y / y_tick_interval) * y_tick_interval

    fig: go.Figure = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=maturities,
            y=rates * 100,
            mode="markers",
            name="Yield",
            marker=dict(
                size=marker_size, color="red", line=dict(width=1, color="black")
            ),
            showlegend=show_legend,
        )
    )

    fig.add_trace(
        go.Scatter(
            x=xs,
            y=ys * 100,
            mode="lines",
            name="Spline Fit",
            line=dict(width=line_width, color="red"),
            showlegend=show_legend,
        )
    )

    if title is not None:
        subtitle_html: str = f"<br><sup>{subtitle}</sup>" if subtitle else ""
        fig.update_layout(title=f"{title}{subtitle_html}")

    fig.update_layout(
        xaxis_title="Maturity (weeks)",
        yaxis=dict(
            title="Coupon Equivalent (%)",
            range=[min_y, max_y],
            dtick=y_tick_interval,
        ),
        template="plotly_white",
        margin=dict(l=70, r=20, t=80, b=60),
    )

    return fig


def plot_spreads_heatmap(
    maturities: npt.NDArray[np.float64],
    yields: npt.NDArray[np.float64],
    title: Optional[str] = None,
    subtitle: Optional[str] = None,
    colorscale: str = "RdBu",
    marker_size: int = 30,
    text_size: int = 8,
    show_values: bool = True,
) -> go.Figure:
    """
    Build an interactive triangular 'heatmap' (scatter) of yield spreads. Spread
    convention: Shorter minus Longer (in percentage points).

    Parameters
    ----------
    maturities : npt.NDArray[np.float64]
        Array of maturity values (in weeks).
    yields : npt.NDArray[np.float64]
        Array of yield values in percentages.
    title : Optional[str]
        Main figure title.
    subtitle : Optional[str]
        Subtitle shown under the main title describing the spread formula.
    colorscale : str
        Any Plotly colorscale, defaults to `RdBu`.
    marker_size : int
        Circle marker size.
    text_size : int
        Text annotation size.
    show_values : bool
        If True, annotate each circle with its spread value.

    Returns
    -------
    fig : plotly.graph_objects.Figure
        Plotly figure.
    """
    labels: List[str] = [
        f"{int(mat)}w" if float(mat).is_integer() else f"{mat}w" for mat in maturities
    ]

    # Upper-triangle indices (short < long)
    coords: List[Tuple[int, int]] = [
        (i, j) for i in range(len(maturities)) for j in range(len(maturities)) if i < j
    ]
    short_indices: npt.NDArray[np.int64] = np.array([i for i, j in coords])
    long_indices: npt.NDArray[np.int64] = np.array([j for i, j in coords])

    # Spread = shorter − longer (percentage points)
    spreads: npt.NDArray[np.float64] = np.array(
        [yields[i] - yields[j] for i, j in coords]
    )

    plot_data: pl.DataFrame = pl.DataFrame(
        {
            "short_index": short_indices,
            "long_index": long_indices,
            "shorter_yield": [yields[i] for i in short_indices],
            "longer_yield": [yields[j] for j in long_indices],
            "spread": spreads,
        }
    )

    # Near the center (where RdBu is light), use black; otherwise white
    # The "light" region heuristic: within 25% of the min/max range around the midpoint
    vmin, vmax = spreads.min(), spreads.max()
    vmid: np.floating = (vmin + vmax) / 2.0
    band: np.floating = 0.25 * (vmax - vmin)
    is_light: npt.NDArray[np.bool_] = (
        plot_data["spread"]
        .is_between(float(vmid - band), float(vmid + band))
        .to_numpy()
    )
    text_colors: npt.NDArray[np.str_] = np.where(is_light, "black", "white")

    mask_white: npt.NDArray[np.bool_] = text_colors == "white"
    mask_black: npt.NDArray[np.bool_] = text_colors == "black"

    def make_trace(
        mask: npt.NDArray[np.bool_], textcolor: str, show_colorbar: bool = False
    ) -> go.Scatter:
        subset: pl.DataFrame = plot_data.filter(mask)
        return go.Scatter(
            x=subset["long_index"],
            y=subset["short_index"],
            mode="markers+text" if show_values else "markers",
            text=[f"{v:.2f}" for v in subset["spread"]] if show_values else None,
            textposition="middle center",
            textfont=dict(color=textcolor, size=text_size),
            marker=dict(
                size=marker_size,
                color=subset["spread"],
                colorscale=colorscale,
                cmin=vmin,
                cmax=vmax,
                reversescale=True,
                line=dict(color="black", width=1),
                colorbar=dict(title=f"% Points") if show_colorbar else None,
                showscale=show_colorbar,
            ),
            hovertemplate=(
                "Shorter: %{customdata[0]:.2f} %<br>"
                "Longer: %{customdata[1]:.2f} %<extra></extra>"
            ),
            customdata=np.stack(
                [subset["shorter_yield"], subset["longer_yield"], subset["spread"]],
                axis=-1,
            ),
            showlegend=False,
        )

    trace_white = make_trace(mask_white, "white", show_colorbar=False)
    trace_black = make_trace(mask_black, "black", show_colorbar=True)

    fig = go.Figure(data=[trace_white, trace_black])

    if title is not None:
        subtitle_html: str = f"<br><sup>{subtitle}</sup>" if subtitle else ""
        fig.update_layout(title=f"{title}{subtitle_html}")

    fig.update_layout(
        xaxis=dict(
            title="Longer Maturity (weeks)",
            tickmode="array",
            tickvals=list(range(len(labels))),
            ticktext=labels,
        ),
        yaxis=dict(
            title="Shorter Maturity (weeks)",
            tickmode="array",
            tickvals=list(range(len(labels))),
            ticktext=labels,
            autorange="reversed",
        ),
        plot_bgcolor="white",
        margin=dict(l=70, r=20, t=80, b=60),
    )

    return fig
