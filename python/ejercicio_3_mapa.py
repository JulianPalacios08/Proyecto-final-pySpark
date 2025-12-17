import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# ===============================
# 1. Rutas
# ===============================
SPARK_OUTPUT = "/home/julian/ghcn/output/extreme_trend_by_station"
MAP_PATH = "/home/julian/ghcn/maps/ne_110m_admin_0_countries.shp"
OUTPUT_IMG = "/home/julian/ghcn/output/ejercicio_3_tendencia_precipitacion.png"

# ===============================
# 2. Leer resultado de Spark
# ===============================
csv_files = [f for f in os.listdir(SPARK_OUTPUT) if f.endswith(".csv")]
if not csv_files:
    raise FileNotFoundError("No se encontró ningún CSV en el output de Spark")

df = pd.read_csv(os.path.join(SPARK_OUTPUT, csv_files[0]))

# Limpieza básica
df = df.dropna(subset=["LATITUDE", "LONGITUDE", "trend"])

# ===============================
# 3. Convertir a GeoDataFrame
# ===============================
gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df.LONGITUDE, df.LATITUDE),
    crs="EPSG:4326"
)

# ===============================
# 4. Cargar mapa base
# ===============================
world = gpd.read_file(MAP_PATH)

# ===============================
# 5. Plot
# ===============================
fig, ax = plt.subplots(1, 1, figsize=(14, 8))

# Mapa base
world.plot(
    ax=ax,
    color="lightgray",
    edgecolor="black"
)

# Puntos de estaciones
gdf.plot(
    ax=ax,
    column="trend",
    cmap="coolwarm",
    markersize=gdf["trend"].abs() * 200,
    legend=True,
    alpha=0.7
)

ax.set_title(
    "Tendencia de Precipitación por Estación (Sudamérica)\n"
    "Valores positivos = aumento | negativos = disminución",
    fontsize=14
)

ax.set_xlabel("Longitud")
ax.set_ylabel("Latitud")

# ===============================
# 6. Guardar imagen (SIN GUI)
# ===============================
plt.tight_layout()
plt.savefig(OUTPUT_IMG, dpi=300)
plt.close()

print(f"✅ Mapa guardado en: {OUTPUT_IMG}")
