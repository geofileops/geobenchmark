"""
Module to prepare test data for benchmarking geo operations.
"""

import enum
import logging
from pathlib import Path
import pprint
import shutil
import tempfile
from typing import Optional, Tuple

import geopandas as gpd
import shapely
import urllib.request
import zipfile

import geofileops as gfo

logger = logging.getLogger(__name__)


class TestFile(enum.Enum):
    AGRIPRC_2018 = (
        0,
        "https://www.landbouwvlaanderen.be/bestanden/gis/Landbouwgebruikspercelen_2018_-_Definitief_(extractie_23-03-2022)_GPKG.zip",
        ".zip",
        "agriprc_2018.gpkg",
    )
    AGRIPRC_2019 = (
        1,
        "https://www.landbouwvlaanderen.be/bestanden/gis/Landbouwgebruikspercelen_2019_-_Definitief_(extractie_20-03-2020)_GPKG.zip",
        ".zip",
        "agriprc_2019.gpkg",
    )
    S2_NDVI_2020 = (
        2,
        "https://www.landbouwvlaanderen.be/bestanden/gis/Droogte%202020%20Sentinel2_NDVI_Periodiek_31370_rgb.zip",
        ".zip",
        "s2_ndvi_2020.tif",
    )
    COMPLEX_POLYS = (3, None, None, "complexpolys.gpkg")

    def __init__(self, value, download_url, download_suffix, dst_name):
        self._value_ = value
        self.download_url = download_url
        self.download_suffix = download_suffix
        self.dst_name = dst_name

    def get_file(self, output_dir: Path) -> Tuple[Path, str]:
        """
        Creates the test file.

        Args:
            tmp_dir (Path): the directory to write the file to.

        Returns:
            _type_: The path to the file + a description of the test file.
        """
        description = None
        if self.download_url is not None:
            testfile_path = download_samplefile(
                download_url=self.download_url,
                download_suffix=self.download_suffix,
                dst_name=self.dst_name,
                dst_dir=output_dir,
            )

            if testfile_path.suffix.lower() == ".gpkg":
                testfile_info = gfo.get_layerinfo(testfile_path)
                logger.debug(
                    f"TestFile {self.name} contains {testfile_info.featurecount} rows."
                )
                description = f"agri parcels, {testfile_info.featurecount} rows"

        elif self.name == "COMPLEX_POLYS":
            # Prepare some complex polygons to test with
            xmin_start = 30000
            step = 20000
            nb_polys = 10
            polys_complex = [
                create_complex_poly(
                    xmin=xmin,
                    ymin=170000.123,
                    width=15000,
                    height=15000,
                    line_distance=500,
                    max_segment_length=100,
                )
                for xmin in range(xmin_start, xmin_start + (nb_polys * step), step)
            ]
            logger.debug(
                f"polys_complex: {len(polys_complex)} polys with num_coordinates: "
                f"{shapely.get_num_coordinates(polys_complex[0])}"
            )
            testfile_path = output_dir / self.dst_name
            complex_gdf = gpd.GeoDataFrame(geometry=polys_complex, crs="epsg:31370")
            complex_gdf.to_file(testfile_path, engine="pyogrio")
            description = (
                f"{len(polys_complex)} complex polys "
                f"(each {shapely.get_num_coordinates(polys_complex[0])} coords)"
            )

        else:
            raise RuntimeError(f"get_file not implemented for {self.name}")

        if description is None:
            description = testfile_path.stem

        return (testfile_path, description)


def create_complex_poly(
    xmin: float,
    ymin: float,
    width: int,
    height: int,
    line_distance: int,
    max_segment_length: int,
) -> shapely.Polygon:
    """Create complex polygon of a ~grid-shape the size specified."""
    lines = []

    # Vertical lines
    for x_offset in range(0, 0 + width, line_distance):
        lines.append(
            shapely.LineString(
                [(xmin + x_offset, ymin), (xmin + x_offset, ymin + height)]
            )
        )

    # Horizontal lines
    for y_offset in range(0, 0 + height, line_distance):
        lines.append(
            shapely.LineString(
                [(xmin, ymin + y_offset), (xmin + width, ymin + y_offset)]
            )
        )

    poly_complex = shapely.unary_union(shapely.MultiLineString(lines).buffer(2))
    poly_complex = shapely.segmentize(
        poly_complex, max_segment_length=max_segment_length
    )
    assert len(shapely.get_parts(poly_complex)) == 1

    return poly_complex


def download_samplefile(
    download_url: str,
    download_suffix: str,
    dst_name: str,
    dst_dir: Optional[Path] = None,
) -> Path:
    """
    Download a sample file to dest_path.

    If it is zipped, it will be unzipped. If needed, it will be converted to
    the file type as determined by the suffix of dst_name.

    Args:
        url (str): the url of the file to download.
        download_name (str): the file name to download to.
        dst_name (str): the file name to save final file to.
        dst_dir (Path): the dir to downloaded the sample file to.
            If it is None, a dir in the default tmp location will be
            used. Defaults to None.

    Returns:
        Path: the path to the downloaded sample file.
    """
    # If the destination path is a directory, use the default file name
    dst_path = prepare_dst_path(dst_name, dst_dir)
    # If the sample file already exists, return
    if dst_path.exists():
        return dst_path
    # Make sure the destination directory exists
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    # If the url points to a file with the same suffix as the dst_path,
    # just download
    if download_suffix.lower() == dst_path.suffix.lower():
        logger.info(f"Download to {dst_path}")
        urllib.request.urlretrieve(download_url, dst_path)
        return dst_path

    # The file downloaded is of a different type than the destination wanted, so some
    # unpacking and/or converting will be needed.
    tmp_dir = dst_path.parent / "tmp"
    try:
        # Remove tmp dir if it exists already
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        # Download file
        tmp_path = tmp_dir / f"{dst_path.stem}{download_suffix.lower()}"
        logger.info(f"Download tmp data to {tmp_path}")
        urllib.request.urlretrieve(download_url, tmp_path)

        # If the temp file is a .zip file, unzip to dir
        if tmp_path.suffix == ".zip":
            # Unzip
            unzippedzip_dir = dst_path.parent / tmp_path.stem
            logger.info(f"Unzip to {unzippedzip_dir}")
            with zipfile.ZipFile(tmp_path, "r") as zip_ref:
                zip_ref.extractall(unzippedzip_dir)

            # Look for the file
            tmp_paths = []
            for suffix in [".shp", ".gpkg", ".tif"]:
                tmp_paths.extend(list(unzippedzip_dir.rglob(f"*{suffix}")))
            if len(tmp_paths) in [1, 2]:
                tmp_path = tmp_paths[0]
            else:
                raise Exception(
                    f"Should find 1 geofile, found {len(tmp_paths)}: \n"
                    f"{pprint.pformat(tmp_paths)}"
                )

        if str(dst_path) != str(tmp_path):
            if dst_path.suffix == tmp_path.suffix:
                if dst_path.suffix == ".tif":
                    shutil.move(tmp_path, dst_path)
                else:
                    gfo.move(tmp_path, dst_path)
            else:
                logger.info(f"Convert tmp file to {dst_path}")
                gfo.makevalid(tmp_path, dst_path)
    finally:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)

    return dst_path


def prepare_dst_path(dst_name: str, dst_dir: Optional[Path] = None):
    if dst_dir is None:
        return Path(tempfile.gettempdir()) / "geofileops_sampledata" / dst_name
    else:
        return dst_dir / dst_name
