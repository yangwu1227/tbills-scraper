from pathlib import Path
from typing import List, Union

import plotly.graph_objects as go
import polars as pl
from plot_utils import plot_spreads_heatmap, plot_yield_curve_spline
from shiny import reactive, render
from shiny.express import render, ui
from shinywidgets import render_plotly

# ---------------------------- MathJax setup (v3) ---------------------------- #

# Load MathJax v3 and configure $...$ / $$...$$
MATHJAX_CONFIG = """
window.MathJax = {
  tex: { 
    inlineMath: [['$', '$'], ['\\\\(', '\\\\)']],
    displayMath: [['$$', '$$'], ['\\\\[', '\\\\]']],
    processEscapes: true,
    processEnvironments: true
  },
  options: { 
    skipHtmlTags: ['script', 'noscript', 'style', 'textarea', 'pre', 'code'],
    ignoreHtmlClass: 'tex2jax_ignore',
    processHtmlClass: 'tex2jax_process'
  },
  startup: {
    ready: () => {
      MathJax.startup.defaultReady();
      // Custom initialization if needed
    }
  }
};
"""

# Re-typeset function for dynamic content
MATHJAX_RETYPESET = """
if (window.MathJax && window.MathJax.typesetPromise) {
  window.MathJax.typesetPromise()
    .then(() => console.log('MathJax typeset complete'))
    .catch((err) => console.error('MathJax typeset error:', err));
}
"""

ui.head_content(
    ui.tags.script(MATHJAX_CONFIG),
    ui.tags.script(src="https://cdn.jsdelivr.net/npm/mathjax@3/es5/tex-chtml.js"),
)

# --------------------------- Reactive data source --------------------------- #


@reactive.calc
def daily_yields() -> pl.DataFrame:
    return pl.read_csv(Path(__file__).parent / "data" / "daily_yields.csv")


@reactive.calc
def break_even_yields() -> pl.DataFrame:
    return pl.read_csv(Path(__file__).parent / "data" / "break_even_yields.csv")


# ---------------------------- Layout: Multi-page ---------------------------- #

with ui.navset_bar(id="pages", title="Daily Treasury Bill Yields"):
    # -------------------------- Page 1: Curve & Spreads ------------------------- #

    with ui.nav_panel("Curve & Spreads"):
        # Two cards side by side
        with ui.layout_columns(fill=True, fillable=True):
            with ui.card(full_screen=True):
                ui.card_header("Yield Curve (Cubic Spline)")

                @render_plotly
                def yield_curve() -> go.Figure:
                    data: pl.DataFrame = daily_yields()
                    fig: go.Figure = plot_yield_curve_spline(
                        maturities=data["maturity"].to_numpy(),
                        yields=data["yield_pct"].to_numpy(),
                        title=None,
                        subtitle=None,
                        n_points=1000,
                        marker_size=5,
                        line_width=2,
                        show_legend=False,
                        y_buffer=0.05,
                        y_tick_interval=0.1,
                    )
                    return fig

                ui.markdown(
                    """
                    - Upward slope → signals growth expectations, risk premium for time (steep slope = strong shift in expectations/risk premium)  
                    - Downward (inverted) slope → signals lower future rates, recession risk  
                    - Flat curve → short- and long-term yields similar, uncertainty  
                    """
                )

            with ui.card(full_screen=True):
                ui.card_header(
                    "Yield Spread (Spread = Shorter % − Longer % Reported in Percentage Points)"
                )

                @render_plotly
                def yield_spread() -> go.Figure:
                    data: pl.DataFrame = daily_yields()
                    fig: go.Figure = plot_spreads_heatmap(
                        maturities=data["maturity"].to_numpy(),
                        yields=data["yield_pct"].to_numpy(),
                        title=None,
                        subtitle=None,
                        colorscale="RdBu",
                        marker_size=40,
                        text_size=12,
                        show_values=True,
                    )
                    return fig

    # ----------------- Page 2: Break-Even Implied Forward Yields ---------------- #

    with ui.nav_panel("Break-Even Implied Forward Yields"):
        with ui.layout_columns(fill=True, fillable=True):
            # Break-even implied forward yield
            with ui.card(full_screen=False):
                ui.card_header("Break-Even Implied Forward Yield (365-day Horizon)")

                @render.data_frame
                def break_even_table() -> render.DataTable:
                    data: pl.DataFrame = break_even_yields()
                    return render.DataTable(
                        data=data,
                        width="100%",
                        height="300px",
                        editable=True,
                    )

        # Interpretation
        with ui.layout_columns(fill=True, fillable=True):
            with ui.card(full_screen=True):
                ui.card_header("Interpretation & Decision Rule")

                @render.ui
                def interpretation() -> List[Union[ui.HTML, ui.Tag]]:
                    content = r"""
                    **What it is.** For any pair *(shorter weeks → longer weeks)*, the break-even implied forward yield, $y_{\mathrm{be}}$, is the **constant annualized yield** that,
                    if we earn it on every short-bill roll (including the final stub), makes our **gross return factor** (wealth multiple) over the horizon **exactly equal** to buying
                    the longer bill and compounding it to the same horizon.

                    - A **stub** is the final partial period of a short-bill investment that does not complete a full roll.
                    - The [coupon-equivalent yield](https://www.investopedia.com/terms/c/couponequivalentrate.asp) (**CEY**) is each bill's yield based on the purchase price, discount,
                      and a 365-366 day count convention.

                    Formally, with day-count base $dc=365$ and horizon $H=365$ days:

                    - Short tenor: maturity $m_s$ days; full rolls $k_s=\left\lfloor \tfrac{H}{m_s}\right\rfloor$; stub $r_s=H-k_s\,m_s$
                    - Long tenor: maturity $m_l$ days; observed coupon-equivalent yield $y_l$; full rolls $k_l=\left\lfloor \tfrac{H}{m_l}\right\rfloor$; stub $r_l=H-k_l\,m_l$

                    The break-even identity used to solve for $y_{\mathrm{be}}$ is

                    $$
                    \bigl(1+y_{\mathrm{be}}\tfrac{m_s}{dc}\bigr)^{k_s}
                    \bigl(1+y_{\mathrm{be}}\tfrac{r_s}{dc}\bigr)
                    \;=\;
                    \bigl(1+y_l\tfrac{m_l}{dc}\bigr)^{k_l}
                    \bigl(1+y_l\tfrac{r_l}{dc}\bigr)
                    $$

                    **How one number covers many rolls.** Even though we may roll the short bill several times, **one** threshold $y_{\mathrm{be}}$ is sufficient: rolling **wins** **if and only if** the **geometric mean** of our realized per-period short-bill returns (each full roll and the stub) **exceeds** the return implied by $y_{\mathrm{be}}$. If it is **below**, rolling the longer bill **wins**.

                    - **$y_{\mathrm{be}}$ is a per-reinvestment hurdle.** At each roll date, if the then-current short-bill CEY is above $y_{\mathrm{be}}$, we stay ahead; if it is below, we fall behind.  
                    - **Today’s cushion.** $\Delta = (\text{today’s short CEY}) - y_{\mathrm{be}}$ is how much the short rate could fall, *on average across rolls*, before rolling no longer beats the longer bill.

                    **Actionable decision rule**

                    1. Fix our horizon at $H=365$ days. Choose a pair *(shorter → longer)* from the table.  
                    2. Read off $y_{\mathrm{be}}$ for that pair. This is our **constant hurdle** for every future roll of the short bill (under the stated horizon and long-leg choice).  
                    3. **Expectation-based decision rule (ex-ante):**

                        - If we **expect** the short-bill CEY to **average at least $y_{\mathrm{be}}$** over the coming year (in geometric-mean terms across all rolls and the stub), we **roll the shorter tenor**.  
                        - If we **expect** the short-bill CEY to **average below $y_{\mathrm{be}}$**, we **buy the longer tenor** now.

                    4. **At each roll date (dynamic policy):** We recompute with current rates and the remaining horizon, then act:

                        - If the then-current short CEY $\ge y_{\mathrm{be}}$ (recomputed), we **keep rolling**.  
                        - If it is $< y_{\mathrm{be}}$, we **switch** into the longer tenor.

                    **Examples (from the table)**

                    - $4 \rightarrow 26$ weeks: $y_{\mathrm{be}}\approx 4.016\%$. If we believe 4-week CEYs will, on average over our rolls, be $\ge 4.016\%$, rolling 4-week should beat buying the 26-week today. If we expect them to average $< 4.016\%$, we buy the 26-week.  
                    - $4 \rightarrow 52$ weeks: $y_{\mathrm{be}}\approx 3.822\%$. If we expect the 4-week CEY to average $\ge 3.822\%$ over the year, rolling 4-week should beat buying the 52-week; otherwise, we lock the 52-week.

                    **What this does and does not assume**

                    - We use **coupon-equivalent yield (CEY)** on a $dc=365$ basis and an **integer-roll + self-consistent stub** convention (the leftover days are invested at the same rate we are solving for).  
                    - We **ignore** taxes, transaction costs, bid-ask, settlement timing, and minimums. Since tax treatment is identical for both legs, the break-even rule is unchanged; when comparing TBills to other investments, we should compare **after-tax** yields.  
                    - **Not a forecast.** $y_{\mathrm{be}}$ is a threshold we compare our expectations to; realized results depend on the **path** of short rates at each roll.  
                    - The computed $y_{\mathrm{be}}$ is specific to the chosen pair $(m_s \rightarrow m_l)$, today’s $y_l$, and the horizon $H$. If any of these change (e.g., when we recompute on a later date), the threshold will generally change as well.

                    **Usage**

                    Use $y_{\mathrm{be}}$ as a single, stable hurdle to drive the choice:

                    - **Expect $\ge y_{\mathrm{be}}$** $\Rightarrow$ **roll the shorter tenor**.  
                    - **Expect $< y_{\mathrm{be}}$** $\Rightarrow$ **buy the longer tenor now**.  

                    We monitor this at each roll; this dashboard will recompute $y_{\mathrm{be}}$ with new rates.
                    """
                    return [
                        ui.markdown(content),
                        ui.tags.script(MATHJAX_RETYPESET),
                    ]
