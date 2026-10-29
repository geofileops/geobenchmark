import numpy as np
import pandas as pd
from matplotlib.figure import Figure

import reporter


def _capture_figures(monkeypatch) -> list[Figure]:
    figures = []
    savefig = Figure.savefig

    def capture_figure(self, *args, **kwargs) -> None:
        figures.append(self)
        savefig(self, *args, **kwargs)

    monkeypatch.setattr(Figure, "savefig", capture_figure)
    return figures


def test_save_chart_masks_and_annotates_values_above_limit(
    monkeypatch, tmp_path
) -> None:
    figures = _capture_figures(monkeypatch)
    df = pd.DataFrame({"seconds": [1.0, 20_000.0, 4.0]})
    original_df = df.copy(deep=True)
    chart_path = tmp_path / "chart.png"

    reporter.save_chart(
        df=df,
        title="Comparison of libraries, time in sec\n",
        output_path=chart_path,
        print_labels_on_points=True,
        point_labels_to_right=True,
        y_value_formatter="{0:.0f}",
        yscale="log",
        y_value_limit=10_000,
        size=(8, 6),
        overflow_axes_top=0.82,
    )

    axes = figures[0].axes[0]
    np.testing.assert_allclose(
        axes.lines[0].get_ydata(), [1.0, np.nan, 4.0], equal_nan=True
    )
    overflow_annotations = [text for text in axes.texts if text.get_text() == "20000"]
    assert len(overflow_annotations) == 1
    annotation = overflow_annotations[0]
    arrows = [
        text
        for text in axes.texts
        if text.get_text() == "" and text.arrow_patch is not None
    ]
    assert len(arrows) == 1
    arrow = arrows[0]
    value_labels = [
        text for text in axes.texts if text.get_text() in {"1", "4", "20000"}
    ]
    assert len(value_labels) == 3
    assert all(text.get_fontweight() == "normal" for text in value_labels)
    assert annotation.arrow_patch is None
    assert annotation.xycoords == ("data", "axes fraction")
    assert annotation.xy[1] > 1
    assert annotation.get_position()[0] == 8
    assert annotation.get_ha() == "left"
    assert arrow.xycoords == ("data", "axes fraction")
    assert arrow.xy[1] > 1
    assert arrow.xy[0] == arrow.get_position()[0]
    assert arrow.xy[1] > annotation.xy[1]
    marker = axes.lines[1]
    assert marker.get_marker() == "o"
    assert marker.get_color() == axes.lines[0].get_color()
    assert marker.get_xdata()[0] > arrow.xy[0]
    assert marker.get_ydata()[0] > 1
    assert annotation.xy[0] == marker.get_xdata()[0]
    figures[0].canvas.draw()
    annotation_bbox = annotation.get_window_extent()
    assert annotation_bbox.y0 > axes.get_window_extent().y1
    title_bbox = axes.title.get_window_extent()
    assert not annotation_bbox.overlaps(title_bbox)
    assert title_bbox.y1 <= figures[0].bbox.y1
    assert figures[0].bbox.y1 - title_bbox.y1 < figures[0].bbox.height * 0.04
    assert axes.get_ylim()[1] < 10_000
    assert chart_path.is_file()
    pd.testing.assert_frame_equal(df, original_df)


def test_save_chart_keeps_values_when_limit_is_not_set(monkeypatch, tmp_path) -> None:
    figures = _capture_figures(monkeypatch)
    df = pd.DataFrame({"seconds": [1.0, 20_000.0]})

    reporter.save_chart(
        df=df,
        title="Benchmark",
        output_path=tmp_path / "chart.png",
        print_labels_on_points=True,
    )

    axes = figures[0].axes[0]
    np.testing.assert_allclose(axes.lines[0].get_ydata(), [1.0, 20_000.0])
    assert axes.get_ylim()[1] > 20_000
    assert all(text.arrow_patch is None for text in axes.texts)


def test_save_chart_labels_each_series_value_once(monkeypatch, tmp_path) -> None:
    figures = _capture_figures(monkeypatch)
    df = pd.DataFrame({"first": [1.0, 20_000.0], "second": [3.0, 40_000.0]})

    reporter.save_chart(
        df=df,
        title="Benchmark",
        output_path=tmp_path / "chart.png",
        print_labels_on_points=True,
        y_value_formatter="{0:.0f}",
        y_value_limit=10_000,
    )

    labels = [text.get_text() for text in figures[0].axes[0].texts]
    assert labels.count("1") == 1
    assert labels.count("3") == 1
    assert labels.count("20000") == 1
    assert labels.count("40000") == 1


def test_save_chart_splits_x_tick_labels_on_underscores(monkeypatch, tmp_path) -> None:
    figures = _capture_figures(monkeypatch)
    df = pd.DataFrame(
        {"seconds": [1.0, 2.0, 3.0]},
        index=["buffer_op", "dissolve_groupby", "clip"],
    )

    reporter.save_chart(
        df=df,
        title="Benchmark",
        output_path=tmp_path / "chart.png",
    )

    tick_labels = [tick.get_text() for tick in figures[0].axes[0].get_xticklabels()]
    assert tick_labels == ["buffer\nop", "dissolve\ngroupby", "clip"]


def test_generate_reports_accepts_duplicate_latest_rows_with_same_description(
    monkeypatch, tmp_path
) -> None:
    results = pd.DataFrame(
        [
            (
                "2026-01-01",
                "geopandas",
                "1.0.1",
                "buffer",
                "Buffer",
                2.0,
                None,
                100_000,
            ),
            (
                "2026-01-01",
                "geopandas",
                "1.2.0",
                "buffer",
                "Buffer",
                1.0,
                None,
                200_000,
            ),
        ],
        columns=[
            "run_datetime",
            "package",
            "package_version",
            "operation",
            "operation_descr",
            "secs_taken",
            "run_details",
            "max_memory_bytes",
        ],
    )
    results_path = tmp_path / "benchmark_results.csv"
    results.to_csv(results_path, index=False)
    charts = []
    monkeypatch.setattr(reporter, "save_chart", lambda **kwargs: charts.append(kwargs))

    reporter.generate_reports(results_path, tmp_path / "reports")

    assert charts[0]["title"] == "geopandas-buffer\n(Buffer)"
    assert "overflow_axes_top" not in charts[0]
    assert charts[1]["title"] == "Comparison of libraries, time in sec\n"
    assert charts[1]["overflow_axes_top"] == 0.82
