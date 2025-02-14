"""Module to prepare test data for benchmarking geo operations."""

import enum
import logging
import pprint
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Optional, Union

import geopandas as gpd
import pyproj
import shapely
import shapely.affinity

import geofileops as gfo

logger = logging.getLogger(__name__)


class TestFile(enum.Enum):
    """Class to create benchmarking test files.

    Args:
        enum (_type_): _description_

    Raises:
        ValueError: _description_
        RuntimeError: _description_

    Returns:
        _type_: _description_
    """

    AGRIPRC_2018 = (
        0,
        "https://www.landbouwvlaanderen.be/bestanden/gis/Landbouwgebruikspercelen_2018_-_Definitief_(extractie_23-03-2022)_GPKG.zip",
        "agriprc_2018.gpkg",
        "agri parcels (~500k poly)",
    )
    AGRIPRC_2019 = (
        1,
        "https://www.landbouwvlaanderen.be/bestanden/gis/Landbouwgebruikspercelen_2019_-_Definitief_(extractie_20-03-2020)_GPKG.zip",
        "agriprc_2019.gpkg",
        "agri parcels (~500k poly)",
    )
    S2_NDVI_2020 = (
        2,
        "https://www.landbouwvlaanderen.be/bestanden/gis/Droogte%202020%20Sentinel2_NDVI_Periodiek_31370_rgb.zip",
        "s2_ndvi_2020.tif",
        "Sentinel 2 NDVI 2020",
    )

    def __init__(self, value, url: str, filename: str, descr: Optional[str]):
        """Create a test file.

        Args:
            value (_type_): _description_
            url (_type_): _description_
            filename (_type_): _description_
            descr (_type_): _description_
        """
        self._value_ = value
        self.url = url
        self.filename = filename
        self.descr = descr

    def get_file(self, output_dir: Path) -> tuple[Path, str]:
        """Creates the test file.

        Args:
            output_dir (Path): the directory to write the file to.

        Returns:
            tuple[Path, str]: The path to the file + a description of the test file.
        """
        testfile_path = _download_samplefile(
            url=self.url, dst_name=self.filename, dst_dir=output_dir
        )
        testfile_info = gfo.get_layerinfo(testfile_path)
        logger.debug(
            f"TestFile {self.name} contains {testfile_info.featurecount} rows."
        )
        count_kilo = f"{int(testfile_info.featurecount / 1000)}k"
        description = f"agri parcels ({count_kilo} polys)"

        return (testfile_path, description)


def create_testfile(
    bbox: tuple[float, float, float, float],
    geoms: int,
    polys_per_geom: int,
    points_per_poly: int,
    poly_width: float = 30_000,
    poly_height: float = 30_000,
    crs: Union[int, str, pyproj.CRS, None] = None,
    dst_dir: Optional[Path] = None,
) -> tuple[Path, str]:
    """Creates a test file.

    Args:
        bbox (tuple[float, float, float, float]): the bounding box of the test file.
        geoms (int): the number of geometries to generate.
        polys_per_geom (int): the number of polygons to use per geometry. If > 1,
            MultiPolygons will be created.
        points_per_poly (int): indication of the number of points each polygon should
            consist of.
        poly_width (float): the width of the polygons. Defaults to 30000.
        poly_height (float): the height of the polygons. Defaults to 30000.
        crs (str): the crs of the test file. Defaults to None.
        dst_dir (Path): the directory to write the file to.

    Returns:
        tuple[Path, str]: The path to the file + a description of the test file.
    """
    # Format file name

    if polys_per_geom == 1:
        poly_str = f"{geoms}polys({points_per_poly}pnts)"
    else:
        poly_str = f"{geoms}multis({polys_per_geom}polys({points_per_poly}pnts))"
    basename = f"testfile_{poly_str}_{bbox[0]}-{bbox[1]}-{bbox[2]}-{bbox[3]}.gpkg"
    testfile_path = _prepare_dst_path(basename, dst_dir=dst_dir)

    # Format file description
    if points_per_poly > 1000:
        nb_points_str = f"{int(points_per_poly / 1000)}k"
    else:
        nb_points_str = f"{points_per_poly}"

    if polys_per_geom == 1:
        descr = f"{geoms} polys of {nb_points_str} coords"
    else:
        descr = f"{geoms} multipolys of {polys_per_geom} * {nb_points_str} coords"

    # If the files exists already, return
    if testfile_path.exists():
        return (testfile_path, descr)

    # Determine the number of polygons we can generate per row and column
    poly_width_step = poly_width + poly_width * 0.1
    poly_height_step = poly_height + poly_height * 0.1

    bbox_width = bbox[2] - bbox[0]
    if poly_width > bbox_width:
        raise ValueError(f"{bbox_width=} is too small for {poly_width=}")
    max_poly_x = int(bbox_width / poly_width_step)

    bbox_height = bbox[3] - bbox[1]
    if poly_height > bbox_height:
        raise ValueError(f"{bbox_height=} is too small for {poly_height=}")
    max_poly_y = int(bbox_height / poly_height_step)

    if polys_per_geom > max_poly_x * max_poly_y:
        raise ValueError(
            f"{polys_per_geom=} is too large for {max_poly_x=} * {max_poly_y=} as "
            "parts of a multipolygon cannot intersect"
        )

    # Create the polygons asked for
    logger.info(
        f"create file with {geoms} (multi)polys of "
        f"{polys_per_geom} * ~{nb_points_str} points"
    )

    # Create a single complex polygon that we can reuse to create all others...
    poly = _create_complex_poly_points(
        xmin=bbox[0],
        ymin=bbox[1],
        width=poly_width,
        height=poly_height,
        nb_points=points_per_poly,
    )

    # Create the polygons. If many are asked, they can overlap, but for performance
    # testing that should not matter.
    result = []
    x = 0
    y = 0
    for _ in range(geoms):
        if polys_per_geom == 1:
            xoff = x * poly_width_step
            yoff = y * poly_height_step
            result.append(shapely.affinity.translate(poly, xoff=xoff, yoff=yoff))

            x, y = _move_xy(x, y, max_poly_x, max_poly_y)
            continue

        # Multipolygons asked, so create them
        polys = []
        for _ in range(polys_per_geom):
            xoff = x * poly_width_step
            yoff = y * poly_height_step
            polys.append(shapely.affinity.translate(poly, xoff=xoff, yoff=yoff))

            # Move to next polygon
            x, y = _move_xy(x, y, max_poly_x, max_poly_y)

        result.append(shapely.MultiPolygon(polys))

    # Write all geometries to a file
    complex_gdf = gpd.GeoDataFrame(geometry=result, crs=crs)
    complex_gdf.to_file(testfile_path, engine="pyogrio")

    return (testfile_path, descr)


def _move_xy(x, y, max_x, max_y):
    # Move to next location in the grid
    if x >= max_x:
        x = 0
        y += 1
    elif y >= max_y:
        y = 0
        x = 0
    else:
        x += 1

    return x, y


def _create_complex_poly_points(
    xmin: float,
    ymin: float,
    width: float,
    height: float,
    nb_points: int,
    nb_points_tol: float = 0.1,
) -> shapely.Polygon:
    nb_points_estimate: float = nb_points
    line_distance_estimate = _estimate_line_distance(nb_points, width)
    nb_points_min = int(nb_points * (1 - nb_points_tol))
    nb_points_max = int(nb_points * (1 + nb_points_tol))

    while True:
        poly_complex = _create_complex_square_poly(
            xmin=xmin,
            ymin=ymin,
            width=width,
            line_distance=line_distance_estimate,
            max_segment_length=100,
        )

        nb_points_created = shapely.get_num_coordinates(poly_complex).item()
        if nb_points_created < nb_points_min:
            # Not enough points... increase nb_points_estimate
            logger.info(
                f"retry: {nb_points_created=} for {line_distance_estimate=} "
                f"and {nb_points_estimate=} not between {nb_points_min=} and "
                f"{nb_points_max=}"
            )
            nb_points_extra = int((nb_points - nb_points_created) / 2)
            nb_points_estimate += nb_points_extra
            line_distance_estimate = _estimate_line_distance(nb_points_estimate, width)
        elif nb_points_created > nb_points_max:
            # Too many points... decrease nb_points_estimate
            logger.info(
                f"retry: {nb_points_created=} for {line_distance_estimate=} "
                f"and {nb_points_estimate=} not between {nb_points_min=} and "
                f"{nb_points_max=}"
            )
            nb_points_less = (nb_points_created - nb_points) / 2
            nb_points_estimate += nb_points_less
            line_distance_estimate = _estimate_line_distance(nb_points_estimate, width)
        else:
            logger.info(
                f"poly_complex ready with {nb_points_created=} for {nb_points=} "
                f"and {line_distance_estimate=}"
            )

            return poly_complex


def _estimate_line_distance(nb_points: float, width: float) -> int:
    # Empirically values found for line_distance and corresponding number of points (for
    # this specific polygon creation function!).
    # Creating a function based on them didn't lead to good results... so use this
    # table and interpolate linearly between the values.
    empirical_values_reference: dict[float, list] = {
        15000: [
            (85_844, 681),
            (3_007, 3_433),
            (2_017, 7_001),
            (693, 15_242),
            (500, 20_778),
            (250, 50_538),
            (156, 90_325),
            (125, 137_058),
            (105, 192_353),
            (62, 307_842),
            (31, 1_201_306),
        ],
        30000: [
            (85844, 1281),
            (3007, 13098),
            (2017, 19893),
            (693, 58012),
            (500, 79338),
            (250, 194658),
            (156, 346453),
            (125, 533298),
            (105, 752770),
        ],
    }

    if width not in empirical_values_reference:
        raise ValueError(
            f"{width=} not supported, options: {list(empirical_values_reference)}"
        )

    empirical_values = empirical_values_reference[width]
    if nb_points < empirical_values[0][1]:
        raise ValueError(
            f"{nb_points=} not supported, minimal value: {empirical_values[0][1]}"
        )
    if nb_points > empirical_values[-1][1]:
        raise ValueError(
            f"{nb_points=} not supported, maximum value: {empirical_values[-1][1]}"
        )

    distance = -2.0
    for x in range(len(empirical_values) - 1):
        (dist_max, nb_points_min), (dist_min, nb_points_max) = (
            empirical_values[x],
            empirical_values[x + 1],
        )
        if nb_points >= nb_points_min and nb_points <= nb_points_max:
            distance = dist_min + (
                (nb_points - nb_points_min) / (nb_points_max - nb_points_min)
            ) * (dist_max - dist_min)
            break

    if distance < 0:
        raise RuntimeError(f"{nb_points=} gave {distance=}: < 0, so invalid")

    return int(distance)


def _create_complex_square_poly(
    xmin: float,
    ymin: float,
    width: float,
    line_distance: int,
    max_segment_length: int,
) -> shapely.Polygon:
    """Create complex square polygon of a ~grid-shape the size specified."""
    lines = []
    height = width

    # Vertical lines
    for x_offset in range(0, 0 + int(width), line_distance):
        lines.append(
            shapely.LineString(
                [(xmin + x_offset, ymin), (xmin + x_offset, ymin + height)]
            )
        )

    # Horizontal lines
    for y_offset in range(0, 0 + int(height), line_distance):
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


def _download_samplefile(
    url: str, dst_name: str, dst_dir: Optional[Path] = None
) -> Path:
    """Download a sample file to dest_path.

    If it is zipped, it will be unzipped. If needed, it will be converted to
    the file type as determined by the suffix of dst_name.

    Args:
        url (str): the url of the file to download.
        dst_name (str): the name of the destination file.
        dst_dir (Path): the dir to downloaded the sample file to.
            If it is None, a dir in the default tmp location will be
            used. Defaults to None.

    Returns:
        Path: the path to the downloaded sample file.
    """
    # If the destination path is a directory, use the default file name
    dst_path = _prepare_dst_path(dst_name, dst_dir)
    # If the sample file already exists, return
    if dst_path.exists():
        return dst_path
    # Make sure the destination directory exists
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    # If the url points to a file with the same suffix as the dst_path,
    # just download
    url_path = Path(url)
    if url_path.suffix.lower() == dst_path.suffix.lower():
        logger.info(f"Download/copy to {dst_path}")
        if url.startswith("http"):
            urllib.request.urlretrieve(url, dst_path)
        else:
            shutil.copy(url, dst_path)
    else:
        # The file downloaded is different that the destination wanted, so some
        # converting will need to be done
        tmp_dir = dst_path.parent / "tmp"

        try:
            # Remove tmp dir if it exists already
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir)
            tmp_dir.mkdir(parents=True, exist_ok=True)

            # Download file
            tmp_path = tmp_dir / f"{dst_path.stem}{url_path.suffix.lower()}"
            logger.info(f"Download/copy tmp data to {tmp_path}")
            if url.startswith("http"):
                urllib.request.urlretrieve(url, tmp_path)
            else:
                shutil.copy(url, tmp_path)

            # If the temp file is a .zip file, unzip to dir
            if tmp_path.suffix == ".zip":
                # Unzip
                unzippedzip_dir = dst_path.parent / tmp_path.stem
                logger.info(f"Unzip to {unzippedzip_dir}")
                with zipfile.ZipFile(tmp_path, "r") as zip_ref:
                    zip_ref.extractall(unzippedzip_dir)

                # Look for the file
                tmp_paths = []
                for suffix in [".shp", ".gpkg"]:
                    tmp_paths.extend(list(unzippedzip_dir.rglob(f"*{suffix}")))
                if len(tmp_paths) == 1:
                    tmp_path = tmp_paths[0]
                else:
                    raise Exception(
                        f"Should find 1 geofile, found {len(tmp_paths)}: \n"
                        f"{pprint.pformat(tmp_paths)}"
                    )

            if dst_path.suffix == tmp_path.suffix:
                gfo.move(tmp_path, dst_path)
            else:
                logger.info(f"Convert tmp file to {dst_path}")
                gfo.makevalid(tmp_path, dst_path, keep_empty_geoms=False)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    return dst_path


def _prepare_dst_path(dst_name: str, dst_dir: Optional[Path] = None):
    if dst_dir is None:
        return Path(tempfile.gettempdir()) / "geofileops_sampledata" / dst_name
    else:
        return dst_dir / dst_name


def _determine_number_points(width: float):
    """Function to determine the number of points for different line distances.

    Useful to fill the empirical_values_reference dict in _estimate_line_distance.
    """
    line_distances = [85_844, 3_007, 2_017, 693, 500, 250, 156, 125, 105, 62, 31]

    for line_distance in line_distances:
        poly_complex = _create_complex_square_poly(
            xmin=0,
            ymin=0,
            width=width,
            line_distance=line_distance,
            max_segment_length=100,
        )

        nb_points_created = shapely.get_num_coordinates(poly_complex).item()
        print(f"({line_distance}, {nb_points_created}),")


if __name__ == "__main__":
    _determine_number_points(30_000)
