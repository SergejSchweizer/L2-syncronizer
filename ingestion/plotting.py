"""Plot generation utilities for fetched candle data."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

from ingestion.spot import SpotCandle

PriceField = Literal["spot", "close", "open", "high", "low"]



def price_value(candle: SpotCandle, price_field: PriceField) -> float:
    """Select a price value from a candle based on requested field."""

    if price_field in {"spot", "close"}:
        return candle.close_price
    if price_field == "open":
        return candle.open_price
    if price_field == "high":
        return candle.high_price
    return candle.low_price



def build_plot_filename(
    exchange: str,
    symbol: str,
    interval: str,
    price_field: PriceField,
) -> str:
    """Build a stable output filename for a candle plot."""

    def safe(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)

    return f"{safe(exchange)}_{safe(symbol)}_{safe(interval)}_{safe(price_field)}.png"



def save_candle_plots(
    candles_by_exchange: dict[str, dict[str, list[SpotCandle]]],
    output_dir: str,
    price_field: PriceField,
) -> list[str]:
    """Render and save price/volume plots for fetched candles.

    Args:
        candles_by_exchange: Nested mapping ``exchange -> symbol -> candles``.
        output_dir: Directory where plots are written.
        price_field: Price field to plot (``spot`` maps to close).

    Returns:
        Absolute plot file paths.

    Raises:
        RuntimeError: If plotting dependency is unavailable.
    """

    try:
        import matplotlib.dates as mdates
        import matplotlib.patches as mpatches
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError as exc:
        raise RuntimeError("matplotlib is required for plotting. Install project dependencies first.") from exc

    plot_root = Path(output_dir)
    plot_root.mkdir(parents=True, exist_ok=True)

    saved_paths: list[str] = []
    for exchange, symbol_map in candles_by_exchange.items():
        for symbol, candles in symbol_map.items():
            if not candles:
                continue

            times = [item.open_time for item in candles]
            prices = [price_value(item, price_field=price_field) for item in candles]
            volumes = [item.volume for item in candles]
            interval = candles[0].interval

            figure, (price_axis, volume_axis) = plt.subplots(
                2,
                1,
                figsize=(12, 7),
                sharex=True,
                gridspec_kw={"height_ratios": [7, 3]},
            )
            figure.patch.set_facecolor("#f7f9fc")

            for axis in (price_axis, volume_axis):
                axis.set_facecolor("#fdfefe")
                axis.grid(alpha=0.2, linestyle="--", linewidth=0.8, color="#8aa0b5")
                axis.spines["top"].set_visible(False)
                axis.spines["right"].set_visible(False)
                axis.spines["left"].set_color("#8aa0b5")
                axis.spines["bottom"].set_color("#8aa0b5")

            price_axis.plot(times, prices, color="#3a86ff", linewidth=4.0, alpha=0.15)
            price_axis.plot(times, prices, color="#1d4ed8", linewidth=2.2)
            price_axis.fill_between(times, prices, min(prices), color="#60a5fa", alpha=0.12)
            price_axis.set_ylabel(f"{price_field} price", color="#334155")
            price_axis.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
            price_axis.set_title(
                f"{exchange.upper()}  {symbol}  ({interval})",
                fontsize=13,
                fontweight="semibold",
                color="#0f172a",
                pad=10,
            )

            time_points = mdates.date2num(times)
            if len(time_points) > 1:
                deltas = [current - previous for previous, current in zip(time_points, time_points[1:])]
                median_delta = sorted(deltas)[len(deltas) // 2]
                bar_width = max(median_delta * 0.58, 0.00005)
            else:
                bar_width = 0.0008

            volume_colors = ["#2f9e44"]
            volume_colors.extend(
                "#2f9e44" if current >= previous else "#c92a2a"
                for previous, current in zip(prices, prices[1:])
            )
            volume_axis.bar(
                times,
                volumes,
                color=volume_colors,
                width=bar_width,
                alpha=0.82,
                linewidth=0.3,
                edgecolor="#ffffff",
            )
            volume_axis.set_ylabel("volume", color="#334155")
            volume_axis.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
            volume_axis.set_xlabel("time (UTC)", color="#334155")
            volume_axis.xaxis.set_major_formatter(mdates.DateFormatter("%m.%Y"))
            volume_axis.legend(
                handles=[
                    mpatches.Patch(color="#2f9e44", label="Price up vs previous candle"),
                    mpatches.Patch(color="#c92a2a", label="Price down vs previous candle"),
                ],
                loc="upper left",
                frameon=True,
                framealpha=0.9,
                facecolor="#ffffff",
                edgecolor="#cbd5e1",
                fontsize=8,
            )

            figure.autofmt_xdate()
            figure.tight_layout()

            file_name = build_plot_filename(
                exchange=exchange,
                symbol=symbol,
                interval=interval,
                price_field=price_field,
            )
            file_path = plot_root / file_name

            figure.savefig(file_path, dpi=180)
            plt.close(figure)
            saved_paths.append(str(file_path.resolve()))

    return saved_paths
