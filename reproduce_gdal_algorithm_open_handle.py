"""Reproduce an open output handle retained by a GDAL Algorithm."""

import json
import os
from pathlib import Path
import tempfile

from osgeo import gdal

gdal.UseExceptions()


with tempfile.TemporaryDirectory(prefix="gdal-algorithm-handle-") as temp_dir:
    temp_path = Path(temp_dir)
    input_path = temp_path / "input.geojson"
    input_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"id": 1},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    output_path = temp_path / "buffered.gpkg"
    algorithm = gdal.alg.vector.buffer(
        input=input_path,
        output=output_path,
        distance=1,
        output_format="GPKG",
        overwrite=True,
    )

    try:
        try:
            output_path.unlink()
        except PermissionError as error:
            print(f"Unlink before Finalize(): blocked as expected ({error})")
        else:
            raise RuntimeError(
                "Unlink succeeded while the GDAL Algorithm was still alive."
            )
    finally:
        algorithm.Finalize()
    output_path.unlink()
    print("Unlink after Finalize(): succeeded")

    context_output_path = temp_path / "buffered_with.gpkg"
    with gdal.alg.vector.buffer(
        input=input_path,
        output=context_output_path,
        distance=1,
        output_format="GPKG",
        overwrite=True,
    ):
        pass
    context_output_path.unlink()

    print(f"GDAL version: {gdal.VersionInfo('RELEASE_NAME')}")
    print("Unlink after leaving with: succeeded")
    print("Reproduced: the output is locked until the Algorithm is finalized.")
