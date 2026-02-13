import streamlit as st
import pandas as pd
from sickle import Sickle
import plotly.express as px
import plotly.graph_objects as go
import re
from collections import Counter

# --- CONFIGURACIÓN INICIAL ---
st.set_page_config(page_title="Auditoría OAI-PMH", layout="wide")
st.title("📊 Auditoría de Calidad de Metadatos (OAI-PMH)")
st.markdown("Herramienta de análisis técnico y consistencia de registros en repositorios DSpace.")

# --- SIDEBAR: PARÁMETROS ---
st.sidebar.header("Conexión")
# URL vacía por defecto como solicitaste
oai_url = st.sidebar.text_input("URL del OAI Base (ej: .../oai/request)", value="")
limit = st.sidebar.slider("Límite de registros a procesar", 100, 5000, 500)
st.sidebar.caption("Nota: Un número mayor a 1000 puede tardar varios minutos dependiendo de la respuesta del servidor.")

# --- FUNCIONES ---

@st.cache_data
def get_repo_info(url):
    """Obtiene la identidad del repositorio mediante el verbo Identify"""
    try:
        sickle = Sickle(url)
        identify = sickle.Identify()
        return {
            "Nombre": getattr(identify, 'repositoryName', 'Desconocido'),
            "Base URL": getattr(identify, 'baseURL', 'Desconocido'),
            "Versión Protocolo": getattr(identify, 'protocolVersion', '2.0'),
            "Admin Email": getattr(identify, 'adminEmail', 'No público'),
            "Granularidad": getattr(identify, 'granularity', 'Desconocido'),
            "Compresión": getattr(identify, 'compression', 'Ninguna')
        }
    except Exception as e:
        return None

@st.cache_data
def harvest_dynamic(url, limit):
    """Cosecha dinámica: Captura cualquier campo que venga en el XML"""
    data = []
    try:
        sickle = Sickle(url)
        iterator = sickle.ListRecords(metadataPrefix='oai_dc', ignore_deleted=True)
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, record in enumerate(iterator):
            if i >= limit:
                break
            
            # Progreso
            progress_bar.progress((i + 1) / limit)
            status_text.text(f"Procesando registro {i+1}...")

            # Estructura base
            row = {
                'identifier': record.header.identifier,
                'datestamp': record.header.datestamp,
            }
            
            # --- EXTRACCIÓN DINÁMICA DE TODOS LOS CAMPOS ---
            # Sickle devuelve los metadatos como un diccionario donde los valores son listas
            # Ej: {'title': ['Titulo 1'], 'subject': ['Física', 'Química']}
            for key, values in record.metadata.items():
                # Unimos los valores múltiples con punto y coma para que quepan en una celda
                if values:
                    row[key] = "; ".join(values)
            
            # Agregamos conteos útiles para análisis posterior (sin guardarlos como texto)
            row['count_creators'] = len(record.metadata.get('creator', []))
            row['count_subjects'] = len(record.metadata.get('subject', []))
            
            data.append(row)
            
        progress_bar.empty()
        status_text.empty()
        return pd.DataFrame(data)

    except Exception as e:
        st.error(f"Error en la conexión o cosecha: {e}")
        return pd.DataFrame()

# --- INTERFAZ PRINCIPAL ---

if st.sidebar.button("Ejecutar Análisis"):
    if not oai_url:
        st.warning("Por favor ingrese una URL válida.")
    else:
        # 1. INFORMACIÓN DEL REPOSITORIO
        with st.spinner('Obteniendo información del servidor...'):
            repo_info = get_repo_info(oai_url)
        
        if repo_info:
            st.subheader("1. Información del Repositorio")
            c1, c2, c3 = st.columns(3)
            c1.info(f"**Nombre:** {repo_info['Nombre']}")
            c2.info(f"**Admin:** {repo_info['Admin Email']}")
            c3.info(f"**Versión OAI:** {repo_info['Versión Protocolo']}")
            
            with st.expander("Ver detalles técnicos del servidor"):
                st.json(repo_info)

            # 2. COSECHA DE REGISTROS
            with st.spinner(f'Descargando y analizando {limit} registros...'):
                df = harvest_dynamic(oai_url, limit)

            if not df.empty:
                # Procesamiento de Fechas para gráficos
                # Buscamos 'date' o 'date.issued' (oai_dc suele usar 'date')
                date_col = 'date' if 'date' in df.columns else None
                
                if date_col:
                    def extract_year(d):
                        match = re.search(r'\d{4}', str(d))
                        return match.group(0) if match else "Sin Año"
                    df['year_extracted'] = df[date_col].apply(extract_year)

                # --- KPIs ---
                st.divider()
                st.subheader("2. Resumen de Métricas")
                k1, k2, k3, k4 = st.columns(4)
                
                k1.metric("Total Registros", len(df))
                
                # Conteo de vacíos
                missing_title = df['title'].isnull().sum() if 'title' in df.columns else len(df)
                k2.metric("Registros sin Título", missing_title, delta_color="inverse")
                
                avg_sub = round(df['count_subjects'].mean(), 1)
                k3.metric("Promedio Materias/Item", avg_sub)
                
                avg_auth = round(df['count_creators'].mean(), 1)
                k4.metric("Promedio Autores/Item", avg_auth)

                # --- VISUALIZACIONES ---
                st.divider()
                st.subheader("3. Análisis Visual")

                tab1, tab2, tab3, tab4 = st.tabs(["Temporalidad", "Tipologías", "Materias", "Completitud"])

                with tab1:
                    if 'year_extracted' in df.columns:
                        year_counts = df['year_extracted'].value_counts().sort_index().reset_index()
                        year_counts.columns = ['Año', 'Cantidad']
                        fig_date = px.bar(year_counts, x='Año', y='Cantidad', title="Distribución de Publicaciones por Año")
                        st.plotly_chart(fig_date, use_container_width=True)
                    else:
                        st.warning("No se encontró campo de fecha estándar ('date') para graficar.")

                with tab2:
                    if 'type' in df.columns:
                        type_counts = df['type'].value_counts().reset_index()
                        type_counts.columns = ['Tipo', 'Cantidad']
                        # Usamos Pie chart para tipos
                        fig_type = px.pie(type_counts, names='Tipo', values='Cantidad', title="Distribución por Tipo de Documento", hole=0.4)
                        st.plotly_chart(fig_type, use_container_width=True)
                    else:
                        st.info("El campo 'type' no está presente en los metadatos.")

                with tab3:
                    if 'subject' in df.columns:
                        # Separar materias compuestas por ; para el conteo real
                        all_subjects = []
                        for sub_str in df['subject'].dropna():
                            all_subjects.extend([s.strip() for s in sub_str.split(';')])
                        
                        if all_subjects:
                            top_subjects = pd.DataFrame(Counter(all_subjects).most_common(20), columns=['Materia', 'Frecuencia'])
                            fig_sub = px.bar(top_subjects, x='Frecuencia', y='Materia', orientation='h', title="Top 20 Materias (Keywords)", text='Frecuencia')
                            fig_sub.update_layout(yaxis={'categoryorder':'total ascending'})
                            st.plotly_chart(fig_sub, use_container_width=True)
                        else:
                            st.info("No se encontraron materias individuales.")
                    else:
                        st.info("El campo 'subject' no está presente.")

                with tab4:
                    # Análisis de densidad de campos (cuántos campos tiene cada registro)
                    # Excluimos columnas técnicas calculadas
                    cols_to_exclude = ['identifier', 'datestamp', 'count_creators', 'count_subjects', 'year_extracted']
                    meta_cols = [c for c in df.columns if c not in cols_to_exclude]
                    
                    completeness = df[meta_cols].notnull().mean().mul(100).sort_values(ascending=True)
                    
                    fig_comp = px.bar(x=completeness.values, y=completeness.index, orientation='h', 
                                      title="Porcentaje de Ocupación por Campo de Metadato",
                                      labels={'x': '% Completitud', 'y': 'Campo Metadato'})
                    st.plotly_chart(fig_comp, use_container_width=True)

                # --- TABLA DE DATOS ---
                st.divider()
                st.subheader("4. Explorador de Registros")
                st.markdown("Visualización de la data cruda cosechada (Top 100 por rendimiento). Descarga disponible.")
                st.dataframe(df.head(100))
                
                # Botón de descarga CSV
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "Descargar Reporte Completo (CSV)",
                    csv,
                    "auditoria_oai.csv",
                    "text/csv",
                    key='download-csv'
                )

        else:
            st.error("No se pudo identificar el repositorio. Verifica la URL.")