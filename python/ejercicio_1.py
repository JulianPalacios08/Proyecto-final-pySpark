import pandas as pd
import geopandas as gpd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Datos procesados
df = pd.read_csv("/home/julian/ghcn/output/hottest_year_by_country.csv")

# Mapa mundial
world = gpd.read_file("/home/julian/ghcn/maps/ne_110m_admin_0_countries.shp")

# Unir por código de país
world["ISO_A2"] = world["ISO_A2"].str.upper()
merged = world.merge(df, left_on="ISO_A2", right_on="country", how="inner")

# Mapa
merged.plot(
    column="year",
    cmap="plasma",
    legend=True,
    figsize=(15, 8)
)

plt.title("Año más caluroso por país (GHCN)")

plt.savefig(
    "/home/julian/ghcn/output/mapa_coropletico_hottest_year.png",
    dpi=300,
    bbox_inches="tight"
)

#plt.show()
