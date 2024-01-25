import geopandas as gpd
import polars as pl
from shapely import wkt



def convert_to_geodf(polars_df: pl.DataFrame) -> gpd.GeoDataFrame:
    """
    Convert a Polars DataFrame to a GeoDataFrame using WKB or WKT transformation.
    """

    # Convert Polars DataFrame to Pandas DataFrame
    df = polars_df.to_pandas()

    # Convert geometry strings back to geometry objects
    if "geometry" in df.columns:
        df["geometry"] = df["geometry"].apply(wkt.loads)

    else:
        raise ValueError("No 'geometry' column found in the DataFrame")

    # Convert back to GeoDataFrame
    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")


def convert_to_polars(gdf: gpd.GeoDataFrame) -> pl.DataFrame:
    """
    Convert a GeoDataFrame to a Polars DataFrame, converting geometries to WKB strings.
    """
    # If geometry conversion is necessary, uncomment the following line
    # gdf["geometry"] = gdf["geometry"].apply(lambda geom: wkb_dumps(geom, hex=True))
    gdf["geometry"] = gdf["geometry"].apply(wkt.dumps)

    # Convert to Polars DataFrame
    return pl.from_pandas(gdf)