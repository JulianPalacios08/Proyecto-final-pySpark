import pandas as pd
import geopandas as gpd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# -----------------------------
# Cargar datos
# -----------------------------
df = pd.read_csv(
    "/home/julian/ghcn/output/extreme_prcp_by_station/extreme_prcp.csv"
)

gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df.LONGITUDE, df.LATITUDE),
    crs="EPSG:4326"
)

world = gpd.read_file(
    "/home/julian/ghcn/maps/ne_110m_admin_0_countries.shp"
)

# -----------------------------
# Plot
# -----------------------------
fig, ax = plt.subplots(figsize=(15, 8))
world.plot(ax=ax, color="lightgray")

gdf.plot(
    ax=ax,
    column="extreme_days",
    cmap="Reds",
    markersize=gdf["extreme_days"] * 0.5,
    legend=True,
    alpha=0.7
)

plt.title("Anomalías de Precipitación Extrema (Últimos 10 Años)")

plt.savefig(
    "/home/julian/ghcn/output/mapa_extreme_prcp.png",
    dpi=300,
    bbox_inches="tight"
)
