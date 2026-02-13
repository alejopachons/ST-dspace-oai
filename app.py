import streamlit as st
import pandas as pd
from sickle import Sickle
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import re

# Configuración de la página
st.set_page_config(page_title="DSpace Health Check", layout="wide")

st.title("🏥 DSpace OAI Health Check")
st.markdown("Analítica cuantitativa de la salud de tus metadatos vía OAI-PMH.")

# --- SIDEBAR: Configuración ---
st.sidebar.header("Configuración de Cosecha")
oai_url = st.sidebar.text_input("URL del OAI (ej: https://repositorio.u.edu/oai/request)", value="https://dspace.mit.edu/oai/request")
limit = st.sidebar.slider("Límite de registros a analizar (Cuidado con la carga)", 100, 5000, 500)
st.sidebar.info("Nota: Para esta demo, limitamos la cosecha para no saturar el servidor.")

# --- FUNCIÓN DE COSECHA (Con Cache) ---
@st.cache_data
def harvest_data(url, limit):
    records_data = []
    try:
        sickle = Sickle(url)
        # Iteramos sobre los registros
        iterator = sickle.ListRecords(metadataPrefix='oai_dc', ignore_deleted=True)
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, record in enumerate(iterator):
            if i >= limit:
                break
            
            # Actualizar barra de progreso
            progress = (i + 1) / limit
            progress_bar.progress(progress)
            status_text.text(f"Cosechando registro {i+1}...")

            # Extracción segura de datos
            metadata = record.metadata
            
            # Lógica básica de extracción (Dublin Core simple)
            row = {
                'id': record.header.identifier,
                'datestamp': record.header.datestamp,
                'title': metadata.get('title', [None])[0],
                'date_issued': metadata.get('date', [None])[0],
                'creators': metadata.get('creator', []),
                'subjects': metadata.get('subject', []),
                'description': metadata.get('description', [None])[0],
                'type': metadata.get('type', [None])[0],
                'language': metadata.get('language', [None])[0],
                'format': metadata.get('format', [None])[0]
            }
            records_data.append(row)
            
        progress_bar.empty()
        status_text.empty()
        return pd.DataFrame(records_data)

    except Exception as e:
        st.error(f"Error al conectar con el OAI: {e}")
        return pd.DataFrame()

# --- LÓGICA PRINCIPAL ---
if st.sidebar.button("Iniciar Diagnóstico"):
    with st.spinner('Conectando al repositorio...'):
        df = harvest_data(oai_url, limit)

    if not df.empty:
        # --- PROCESAMIENTO ADICIONAL ---
        # Limpiar fechas para obtener solo el año
        def extract_year(date_str):
            if not date_str: return "Sin Fecha"
            match = re.search(r'\d{4}', str(date_str))
            return match.group(0) if match else "Formato Inválido"

        df['year'] = df['date_issued'].apply(extract_year)
        df['creator_count'] = df['creators'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        df['subject_count'] = df['subjects'].apply(lambda x: len(x) if isinstance(x, list) else 0)

        # --- KPI's DE SALUD ---
        st.divider()
        st.subheader("1. Signos Vitales (Resumen)")
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Registros Analizados", len(df))
        
        # Salud de Títulos (Deben existir)
        missing_titles = df['title'].isnull().sum()
        col2.metric("Sin Título (Crítico)", missing_titles, delta_color="inverse")
        
        # Salud de Fechas
        missing_dates = df[df['year'] == "Sin Fecha"].shape[0]
        col3.metric("Sin Fecha", missing_dates, delta_color="inverse")

        # Salud de Autores
        avg_authors = round(df['creator_count'].mean(), 2)
        col4.metric("Promedio Autores/Item", avg_authors)

        # --- GRÁFICOS ---
        
        # 2. Análisis Temporal (Consistencia de ingresos)
        st.subheader("2. Ritmo Cardíaco del Repositorio (Publicaciones por Año)")
        if 'year' in df.columns:
            year_counts = df['year'].value_counts().sort_index().reset_index()
            year_counts.columns = ['Año', 'Cantidad']
            fig_time = px.bar(year_counts, x='Año', y='Cantidad', title="Distribución Temporal")
            st.plotly_chart(fig_time, use_container_width=True)

        # 3. Densidad de Metadatos (Salud del Registro)
        st.subheader("3. Nutrición del Registro (Campos llenos vs vacíos)")
        
        # Calculamos porcentaje de completitud de campos clave
        fields_to_check = ['description', 'language', 'format', 'type']
        completeness = {}
        for field in fields_to_check:
            present = df[field].notnull().sum()
            completeness[field] = (present / len(df)) * 100
        
        fig_health = go.Figure(go.Bar(
            x=list(completeness.values()),
            y=list(completeness.keys()),
            orientation='h',
            marker=dict(color=list(completeness.values()), colorscale='Viridis')
        ))
        fig_health.update_layout(title="Porcentaje de registros con campos presentes", xaxis_title="% Completitud")
        st.plotly_chart(fig_health, use_container_width=True)

        # 4. Tabla de "Pacientes Enfermos"
        st.subheader("4. Triage: Registros que requieren atención")
        st.write("Registros con problemas potenciales (sin fecha, sin descripción o sin autores):")
        
        problematic_df = df[
            (df['year'] == "Sin Fecha") | 
            (df['description'].isnull()) | 
            (df['creator_count'] == 0)
        ]
        
        if not problematic_df.empty:
            st.dataframe(problematic_df[['id', 'title', 'year', 'creator_count', 'description']].head(50))
        else:
            st.success("¡Increíble! En esta muestra no se detectaron registros con problemas graves básicos.")

    else:
        st.warning("No se pudieron cargar datos. Verifica la URL.")