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
oai_url = st.sidebar.text_input("URL del OAI Base (ej: .../oai/request)", value="")
limit = st.sidebar.slider("Límite de registros a procesar", 100, 5000, 500)
st.sidebar.caption("Nota: Un número mayor a 1000 puede tardar varios minutos.")

# --- FUNCIONES ---

@st.cache_data
def get_repo_info(url):
    """Obtiene la identidad del repositorio"""
    try:
        sickle = Sickle(url)
        identify = sickle.Identify()
        return {
            "Nombre": getattr(identify, 'repositoryName', 'Desconocido'),
            "Base URL": getattr(identify, 'baseURL', 'Desconocido'),
            "Versión Protocolo": getattr(identify, 'protocolVersion', '2.0'),
            "Admin Email": getattr(identify, 'adminEmail', 'No público')
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

            row = {
                'identifier': record.header.identifier,
                'datestamp': record.header.datestamp,
            }
            
            # Extracción dinámica
            for key, values in record.metadata.items():
                if values:
                    # Guardamos unido por ; para la tabla visual
                    row[key] = "; ".join(values)
            
            # Conteos para análisis numérico
            row['count_creators'] = len(record.metadata.get('creator', []))
            row['count_subjects'] = len(record.metadata.get('subject', []))
            
            data.append(row)
            
        progress_bar.empty()
        status_text.empty()
        return pd.DataFrame(data)

    except Exception as e:
        st.error(f"Error en la conexión o cosecha: {e}")
        return pd.DataFrame()

def split_and_count(df, column, top_n=20):
    """Función auxiliar para separar valores con ';' y contarlos individualmente"""
    if column not in df.columns:
        return pd.DataFrame()
    
    all_items = []
    for item_str in df[column].dropna():
        # Separamos por punto y coma y limpiamos espacios
        items = [i.strip() for i in str(item_str).split(';')]
        all_items.extend(items)
    
    if not all_items:
        return pd.DataFrame()
        
    counts = pd.DataFrame(Counter(all_items).most_common(top_n), columns=['Valor', 'Frecuencia'])
    return counts

# --- INTERFAZ PRINCIPAL ---

if st.sidebar.button("Ejecutar Análisis"):
    if not oai_url:
        st.warning("Por favor ingrese una URL válida.")
    else:
        # 1. INFO DEL REPOSITORIO
        with st.spinner('Obteniendo información del servidor...'):
            repo_info = get_repo_info(oai_url)
        
        if repo_info:
            st.subheader("1. Información del Repositorio")
            c1, c2, c3 = st.columns(3)
            c1.info(f"**Nombre:** {repo_info['Nombre']}")
            c2.info(f"**Admin:** {repo_info['Admin Email']}")
            c3.info(f"**Versión OAI:** {repo_info['Versión Protocolo']}")
            
            # 2. COSECHA
            with st.spinner(f'Descargando y analizando {limit} registros...'):
                df = harvest_dynamic(oai_url, limit)

            if not df.empty:
                # Procesamiento de Fecha (Año)
                date_col = 'date' if 'date' in df.columns else None
                if date_col:
                    def extract_year(d):
                        match = re.search(r'\d{4}', str(d))
                        return match.group(0) if match else "Sin Año"
                    df['year_extracted'] = df[date_col].apply(extract_year)

                # --- 2. RESUMEN DE MÉTRICAS (KPIs MODIFICADOS) ---
                st.divider()
                st.subheader("2. Resumen de Métricas")
                k1, k2, k3, k4 = st.columns(4)
                
                k1.metric("Total Registros", len(df))
                
                missing_title = df['title'].isnull().sum() if 'title' in df.columns else len(df)
                k2.metric("Sin Título", missing_title, delta_color="inverse")
                
                missing_desc = df['description'].isnull().sum() if 'description' in df.columns else len(df)
                k3.metric("Sin Descripción", missing_desc, delta_color="inverse")
                
                missing_date = df[df['year_extracted'] == "Sin Año"].shape[0] if 'year_extracted' in df.columns else 0
                k4.metric("Sin Fecha (Año)", missing_date, delta_color="inverse")

                # --- 3. ANÁLISIS VISUAL ---
                st.divider()
                st.subheader("3. Análisis Visual")

                tab1, tab2, tab3, tab4, tab5 = st.tabs([
                    "Temporalidad", 
                    "Tipologías y Formatos", 
                    "Materias", 
                    "Volumen de Datos",
                    "Completitud (Semáforo)"
                ])

                # TAB 1: TEMPORALIDAD
                with tab1:
                    if 'year_extracted' in df.columns:
                        year_counts = df['year_extracted'].value_counts().sort_index().reset_index()
                        year_counts.columns = ['Año', 'Cantidad']
                        fig_date = px.bar(year_counts, x='Año', y='Cantidad', title="Publicaciones por Año")
                        st.plotly_chart(fig_date, use_container_width=True)
                    else:
                        st.warning("No se encontró campo de fecha.")

                # TAB 2: TIPOLOGÍAS Y FORMATOS (Lógica Split corregida)
                with tab2:
                    col_t1, col_t2 = st.columns(2)
                    
                    with col_t1:
                        st.markdown("#### Tipologías (Type)")
                        if 'type' in df.columns:
                            # Usamos la función helper para separar los valores concatenados
                            type_data = split_and_count(df, 'type')
                            if not type_data.empty:
                                fig_type = px.pie(type_data, names='Valor', values='Frecuencia', hole=0.4)
                                st.plotly_chart(fig_type, use_container_width=True)
                            else:
                                st.info("Datos de 'type' vacíos.")
                        else:
                            st.info("Campo 'type' no encontrado.")
                            
                    with col_t2:
                        st.markdown("#### Idiomas (Language)")
                        if 'language' in df.columns:
                            lang_data = split_and_count(df, 'language')
                            if not lang_data.empty:
                                fig_lang = px.pie(lang_data, names='Valor', values='Frecuencia', hole=0.4)
                                st.plotly_chart(fig_lang, use_container_width=True)
                            else:
                                st.info("Datos de 'language' vacíos.")
                        else:
                            st.info("Campo 'language' no encontrado.")

                    st.markdown("#### Formatos (Format)")
                    if 'format' in df.columns:
                        fmt_data = split_and_count(df, 'format')
                        if not fmt_data.empty:
                            fig_fmt = px.bar(fmt_data, x='Frecuencia', y='Valor', orientation='h')
                            st.plotly_chart(fig_fmt, use_container_width=True)
                    else:
                        st.info("Campo 'format' no encontrado.")

                # TAB 3: MATERIAS (Lógica Split corregida)
                with tab3:
                    if 'subject' in df.columns:
                        sub_data = split_and_count(df, 'subject', top_n=30)
                        if not sub_data.empty:
                            fig_sub = px.bar(sub_data, x='Frecuencia', y='Valor', orientation='h', 
                                             title="Top 30 Materias (Separadas individualmente)", text='Frecuencia')
                            fig_sub.update_layout(yaxis={'categoryorder':'total ascending'})
                            st.plotly_chart(fig_sub, use_container_width=True)
                        else:
                            st.info("No hay datos de materias.")
                    else:
                        st.info("Campo 'subject' no encontrado.")

                # TAB 4: VOLUMEN DE DATOS (Nuevas gráficas solicitadas)
                with tab4:
                    c_v1, c_v2 = st.columns(2)
                    with c_v1:
                        fig_auth_hist = px.histogram(df, x="count_creators", nbins=20, 
                                                     title="Distribución: Nº Autores por Registro",
                                                     labels={'count_creators': 'Cantidad de Autores'})
                        st.plotly_chart(fig_auth_hist, use_container_width=True)
                    
                    with c_v2:
                        fig_sub_hist = px.histogram(df, x="count_subjects", nbins=20, 
                                                    title="Distribución: Nº Materias por Registro",
                                                    labels={'count_subjects': 'Cantidad de Materias'})
                        st.plotly_chart(fig_sub_hist, use_container_width=True)

                # TAB 5: COMPLETITUD (Semáforo)
                with tab5:
                    cols_to_exclude = ['identifier', 'datestamp', 'count_creators', 'count_subjects', 'year_extracted']
                    meta_cols = [c for c in df.columns if c not in cols_to_exclude]
                    
                    # Calcular porcentaje
                    completeness_series = df[meta_cols].notnull().mean().mul(100).sort_values(ascending=True)
                    
                    # Crear colores según valor
                    colors = []
                    for val in completeness_series.values:
                        if val < 80:
                            colors.append('#FF4B4B') # Rojo Streamlit
                        elif val < 100:
                            colors.append('#FFAA00') # Amarillo/Naranja
                        else:
                            colors.append('#09AB3B') # Verde

                    fig_comp = go.Figure(go.Bar(
                        x=completeness_series.values,
                        y=completeness_series.index,
                        orientation='h',
                        marker_color=colors,
                        text=[f"{v:.1f}%" for v in completeness_series.values],
                        textposition='auto'
                    ))
                    
                    fig_comp.update_layout(
                        title="Nivel de Completitud por Campo (<80% Crítico)",
                        xaxis_title="% de Registros con el campo presente",
                        xaxis=dict(range=[0, 100])
                    )
                    st.plotly_chart(fig_comp, use_container_width=True)

                # --- 4. DATA RAW ---
                st.divider()
                st.subheader("4. Explorador de Datos")
                st.dataframe(df.head(100))
                
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "Descargar CSV",
                    csv,
                    "auditoria_oai_clean.csv",
                    "text/csv"
                )
        else:
            st.error("No se pudo conectar al repositorio.")