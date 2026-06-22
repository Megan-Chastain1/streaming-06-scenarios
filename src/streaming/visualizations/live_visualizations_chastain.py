"""src/streaming/visualizations/live_visualizations_case.py.

Project-specific live visualization functions used by the Kafka consumer.

This module creates a live line chart of sale total by message.
The chart opens in a window while the consumer is running and updates
as each message is consumed.

Author: Megan Chastain
Date: 2026-06

"""


# === DECLARE IMPORTS ===

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt

# === DECLARE EXPORTS ===

# Use the built-in __all__ variable to declare a list of
# public objects that this module exports.
# This is a common Python convention that helps other developers understand
# which functions are intended for use outside this module.

__all__ = [
    "close_live_chart",
    "init_live_chart",
    "save_live_chart",
    "update_live_chart",
]


# === DEFINE LIVE CHART HELPERS ===


def init_live_chart() -> tuple[Any, Any, list[str], list[float]]:
    """Create and show an empty live chart.

    Returns:
        A tuple of (figure, axis, x_values, y_values).
    """
    # Matplotlib has a ion() function built in for "interactive ON" mode,
    # which allows the chart to update in real time as we modify it.
    # Call this function to turn on interactive mode.
    plt.ion()

    # Call subplots() to create a figure and axis for the chart.
    figure, axis = plt.subplots()

    # Initialize empty lists for x and y values.
    # These will be updated as messages are consumed.
    x_values: list[str] = []
    y_values: list[float] = []

    # Per-axis state for coloring by region: a mapping from region_id -> color
    # and a list of colors for each plotted point. We attach these to the axis
    # object so they persist across update_live_chart() calls without changing
    # the public function signatures used elsewhere.

    axis._region_color_map = {}
    axis._point_colors = []

    # Set the title and axis labels for the chart.
    axis.set_title("Sales Total by Region")
    axis.set_xlabel("Region")
    axis.set_ylabel("Sale Total ($)")

    # Call the figure.show() method to display the chart window.
    figure.show()

    # Call the figure.canvas.draw() method to
    # ensure the chart is rendered and responsive.
    figure.canvas.draw()

    # Call the figure.canvas.flush_events() method to process any pending GUI events,
    # which helps the chart window to update properly.
    figure.canvas.flush_events()

    # Return the figure, axis, and the x and y value lists for later use.
    return figure, axis, x_values, y_values


def update_live_chart(
    *,
    figure: Any,
    axis: Any,
    x_values: list[str],
    y_values: list[float],
    message: dict[str, Any],
) -> None:
    """Update the live chart with one consumed message.

    All arguments after the asterisk (*) must be passed as keyword arguments.

    Arguments:
        figure: Matplotlib figure.
        axis: Matplotlib axis.
        x_values: List of x-axis values already shown.
        y_values: List of y-axis values already shown.
        message: One enriched Kafka message dictionary.

    Returns:
        None.
    """
    # Use the region identifier from the message as the x-axis value
    # and as the key for the color mapping.
    region_label = str(message.get("region_id", "unknown"))

    # Create a new y value from the "total" field in the message,
    # which contains the sale total for that message.
    new_y = float(message["total"])

    # Check if region already exists in x_values
    if region_label in x_values:
        # Find the index of the existing region and add to its total
        region_idx = x_values.index(region_label)
        y_values[region_idx] += new_y
    else:
        # Add new region
        x_values.append(region_label)
        y_values.append(new_y)

        # Determine color for this region. Use a qualitative colormap
        # (tab10) and assign colors deterministically as new regions appear.
        cmap = plt.get_cmap("tab10")
        region_map = getattr(axis, "_region_color_map", {})
        idx = len(region_map) % cmap.N
        color = cmap(idx)
        region_map[region_label] = color
        axis._region_color_map = region_map

        # Track point color for each region.
        axis._point_colors.append(color)

    # Clear the axis
    axis.clear()

    # Re-plot the updated x and y values as a bar chart with colored bars.
    x_pos = list(range(len(x_values)))
    axis.bar(
        x_pos,
        y_values,
        color=axis._point_colors,
        edgecolor="k",
        alpha=0.8,
        width=0.6,
    )

    # Show the original region labels on the x-axis.
    axis.set_xticks(x_pos)
    axis.set_xticklabels(x_values)

    # Draw a legend mapping colors to region labels.
    from matplotlib.patches import Patch as _Patch

    region_map = getattr(axis, "_region_color_map", {})
    if region_map:
        handles = [
            _Patch(facecolor=color, edgecolor="k", label=region)
            for region, color in region_map.items()
        ]
        # Place legend outside the plot to the right.
        axis.legend(
            handles=handles, title="Region", bbox_to_anchor=(1.05, 1), loc="upper left"
        )

    # Set the title and axis labels again after clearing the axis.
    axis.set_title("Sales Total by Region")
    axis.set_xlabel("Region")
    axis.set_ylabel("Sale Total ($)")

    # Add a grid to the chart for better readability.
    axis.grid(True)

    # Rotate x tick labels for readability when region names are long.
    import matplotlib.pyplot as _plt

    _plt.xticks(rotation=45)

    # Call the figure.canvas.draw() method to update the chart with the new data.
    figure.canvas.draw()

    # Call the figure.canvas.flush_events() method to process any pending GUI events,
    # which helps the chart to update properly.
    figure.canvas.flush_events()

    # Call plt.pause() with a short time (e.g., 0.05 seconds) to allow the chart to update.
    plt.pause(0.05)


def save_live_chart(
    *,
    figure: Any,
    chart_path: Path,
) -> None:
    """Save the final live chart to an image file.

    All arguments after the asterisk (*) must be passed as keyword arguments.

    Arguments:
        figure: Matplotlib figure.
        chart_path: Output image path.

    Returns:
        None.
    """
    # Ensure the output directory exists before saving the figure.
    chart_path.parent.mkdir(parents=True, exist_ok=True)

    # Use the figure.savefig() method to save the chart to an image file.
    # Use the bbox_inches="tight" argument to ensure the saved image is cropped to the content of the chart.
    figure.savefig(chart_path, bbox_inches="tight")


def close_live_chart() -> None:
    """Turn off interactive chart mode."""
    # Call plt.ioff() to turn off interactive mode when the consumer is finished.
    plt.ioff()
