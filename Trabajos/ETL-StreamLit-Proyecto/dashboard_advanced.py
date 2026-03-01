#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
import os

# ==============================
# CONFIG STREAMLIT
# ==============================

st.set_page_config(
    page_title="Dashboard NASA NeoWs",
    page_icon="☄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("☄️ Dashboard NASA - Near Earth Objects (NeoWs)")
st.markdown("Análisis de asteroides cercanos a la Tierra")
st.markdown("---")

# ==============================
# CARGA CSV
# ==============================

CSV_PATH = "../etlweatherstack/ETL-proyecto/NASA/data/asteroides.csv"  # Ajusta si está en otra ruta

if not os.path.exists(CSV_PATH):
    st.error(f"No se encontró el archivo CSV en: {CSV_PATH}")
    st.stop()

df = pd.read_csv(CSV_PATH)

if df.empty:
    st.warning("El CSV está vacío.")
    st.stop()

# ==============================
# LIMPIEZA Y TIPOS
# ==============================

df["fecha_aproximacion"] = pd.to_datetime(df["fecha_aproximacion"])
df["fecha_extraccion"] = pd.to_datetime(df["fecha_extraccion"])
df["es_peligroso"] = df["es_peligroso"].astype(bool)

# Crear diámetro promedio
df["diametro_promedio_km"] = (
    df["diametro_min_km"] + df["diametro_max_km"]
) / 2

# ==============================
# SIDEBAR FILTROS
# ==============================

st.sidebar.title("🔎 Filtros")

# Filtro por fecha
fecha_min = df["fecha_aproximacion"].min()
fecha_max = df["fecha_aproximacion"].max()

rango_fechas = st.sidebar.date_input(
    "Rango de Fecha de Aproximación",
    [fecha_min, fecha_max]
)

df_filtrado = df[
    (df["fecha_aproximacion"] >= pd.to_datetime(rango_fechas[0])) &
    (df["fecha_aproximacion"] <= pd.to_datetime(rango_fechas[1]))
]

# Filtro por peligrosidad
filtro_peligroso = st.sidebar.selectbox(
    "Filtrar por peligro:",
    ["Todos", "Solo Peligrosos", "Solo No Peligrosos"]
)

if filtro_peligroso == "Solo Peligrosos":
    df_filtrado = df_filtrado[df_filtrado["es_peligroso"] == True]
elif filtro_peligroso == "Solo No Peligrosos":
    df_filtrado = df_filtrado[df_filtrado["es_peligroso"] == False]

if df_filtrado.empty:
    st.warning("No hay datos con los filtros seleccionados.")
    st.stop()

# ==============================
# MÉTRICAS PRINCIPALES
# ==============================

st.subheader("📊 Métricas Generales")

col1, col2, col3, col4 = st.columns(4)

total = len(df_filtrado)
peligrosos = df_filtrado["es_peligroso"].sum()
velocidad_max = df_filtrado["velocidad_km_s"].max()
distancia_min = df_filtrado["distancia_km"].min()

with col1:
    st.metric("☄️ Total Asteroides", total)

with col2:
    st.metric("⚠️ Potencialmente Peligrosos", int(peligrosos))

with col3:
    st.metric("🚀 Velocidad Máxima (km/s)", f"{velocidad_max:.2f}")

with col4:
    st.metric("📏 Distancia Mínima (km)", f"{distancia_min:,.0f}")

st.markdown("---")

# ==============================
# VISUALIZACIONES
# ==============================

st.subheader("📈 Visualizaciones")

col1, col2 = st.columns(2)

# Magnitud absoluta
with col1:
    fig_mag = px.histogram(
        df_filtrado,
        x="magnitud_absoluta",
        nbins=20,
        title="Distribución de Magnitud Absoluta"
    )
    st.plotly_chart(fig_mag, use_container_width=True)

# Diámetro vs Velocidad
with col2:
    fig_scatter = px.scatter(
        df_filtrado,
        x="diametro_promedio_km",
        y="velocidad_km_s",
        color="es_peligroso",
        size="distancia_km",
        hover_data=["nombre"],
        title="Diámetro vs Velocidad"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

st.markdown("---")

# ==============================
# TOP 10 MÁS RÁPIDOS
# ==============================

st.subheader("🚀 Top 10 Asteroides Más Rápidos")

top_10 = df_filtrado.sort_values(
    "velocidad_km_s",
    ascending=False
).head(10)

st.dataframe(
    top_10[[
        "nombre",
        "velocidad_km_s",
        "distancia_km",
        "es_peligroso",
        "fecha_aproximacion"
    ]],
    use_container_width=True
)

st.markdown("---")

# ==============================
# TABLA COMPLETA
# ==============================

st.subheader("📋 Datos Completos")

st.dataframe(
    df_filtrado.sort_values(
        "fecha_aproximacion",
        ascending=False
    ),
    use_container_width=True,
    height=400
)