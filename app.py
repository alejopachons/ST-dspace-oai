import streamlit as st
import pandas as pd
from sickle import Sickle
import plotly.express as px
import plotly.graph_objects as go
import re
from collections import Counter
import math

# --- CONFIGURACIÓN INICIAL ---
st.set_page_config(page_title="Auditoría OAI-PMH", layout="wide")
st.title("📊 Auditoría de Calidad de Metadatos (OAI-PMH)")
st.markdown("Herramienta de análisis técnico y consistencia de registros para Administradores de Repositorios.")

# --- GESTIÓN DE ESTADO ---
if 'repo_info' not in st.session_state:
    st.session_state.repo_info = None

# --- FUNCIONES DE PROCESAMIENTO ---

def get_repo_info(url):
    try:
        sickle = Sickle(url)
        identify = sickle.Identify()
        return {
            "Nombre": getattr(identify, 'repositoryName', 'Desconocido'),
            "Base URL": getattr(identify, 'baseURL', 'Desconocido'),
            "Versión Protocolo": getattr(identify, 'protocolVersion', '2.0'),
            "Repository ID": getattr(identify, 'repositoryIdentifier', None)
        }
    except Exception as e:
        return None

def detect_clean_format(format_list_str):
    """
    Intenta deducir el formato real del archivo basándose en cadenas comunes 
    dentro del campo dc.format (MIME types o extensiones).
    """
    if not format_list_str or pd.isna(format_list_str):
        return "No Especificado"
    
    text = str(format_list_str).lower()
    
    # Prioridad de detección
    if 'pdf' in text or 'application/pdf' in text:
        return 'PDF'
    if 'xml' in text:
        return 'XML'
    if any(x in text for x in ['jpg', 'jpeg', 'png', 'gif', 'image']):
        return 'Imagen'
    if 'word' in text or 'doc' in text or 'docx' in text:
        return 'Word'
    if 'excel' in text or 'xls' in text:
        return 'Excel'
    if 'zip' in text or 'rar' in text:
        return 'Archivo Comprimido'
    if 'mp4' in text or 'video' in text:
        return 'Video'
    if 'mp3' in text or 'audio' in text:
        return 'Audio'
    
    return 'Otros/Desconocido'

@st.cache_data(show_spinner=False)
def harvest_dynamic(url, limit):
    data = []
    try:
        sickle = Sickle(url)
        iterator = sickle.ListRecords(metadataPrefix='oai_dc', ignore_deleted=True)
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, record in enumerate(iterator):
            if i >= limit:
                break
            
            if i % 10 == 0:
                progress = min((i + 1) / limit, 1.0)
                progress_bar.progress(progress)
                status_text.text(f"Cosechando registro {i+1} de {limit}...")

            row = {
                'identifier': record.header.identifier,
                'datestamp': record.header.datestamp,
            }
            
            for key, values in record.metadata.items():
                if values:
                    clean_values = [str(v) for v in values if v is not None]
                    if clean_values:
                        row[key] = "; ".join(clean_values)
            
            # Conteos
            row['count_creators'] = len(record.metadata.get('creator', []))
            row['count_subjects'] = len(record.metadata.get('subject', []))
            
            data.append(row)
            
        progress_bar.progress(1.0)
        status_text.text("Cosecha completada.")
        status_text.empty()
        return pd.DataFrame(data)

    except Exception as e:
        st.error(f"Error en la conexión o cosecha: {e}")
        return pd.DataFrame()

def split_and_count(df, column, top_n=20):
    if column not in df.columns:
        return pd.DataFrame()
    
    all_items = []
    for item_str in df[column].dropna():
        items = [i.strip() for i in str(item_str).split(';')]
        for i in items:
            if not i.startswith("info:eu-repo") and not i.startswith("http"):
                all_items.append(i)
    
    if not all_items:
        return pd.DataFrame()
        
    counts = pd.DataFrame(Counter(all_items).most_common(top_n), columns=['Valor', 'Frecuencia'])
    return counts

# --- SIDEBAR: CONEXIÓN ---
st.sidebar.header("1. Conexión")
oai_url = st.sidebar.text_input("URL del OAI Base", value="", help="Ej: https://repositorio.u.edu/oai/request")

if st.sidebar.button("Verificar Conexión"):
    if oai_url:
        with st.spinner("Conectando..."):
            info = get_repo_info(oai_url)
            if info:
                st.session_state.repo_info = info
                st.sidebar.success("¡Conectado!")
            else:
                st.sidebar.error("No se pudo conectar.")

# Configuración de límite
limit = 500
if st.session_state.repo_info:
    repo_id = st.session_state.repo_info.get('Repository ID', 'Desconocido')
    st.sidebar.divider()
    st.sidebar.header("2. Configuración de Cosecha")
    
    high_vol_mode = st.sidebar.checkbox("🔓 Habilitar Cosecha Masiva (> 5000)")
    if high_vol_mode:
        st.sidebar.info(f"ID del repositorio: **{repo_id}**")
        security_check = st.sidebar.text_input("Ingrese Repository Identifier")
        if security_check.strip() == repo_id:
            limit = st.sidebar.number_input("Cantidad de registros", min_value=5000, max_value=50000, value=5000, step=1000)
        else:
            limit = st.sidebar.slider("Límite seguro", 100, 5000, 500)
    else:
        limit = st.sidebar.slider("Límite de registros", 100, 5000, 500)

run_analysis = st.sidebar.button("🚀 Iniciar Auditoría", type="primary")

# --- LÓGICA PRINCIPAL ---

if run_analysis:
    if not oai_url:
        st.warning("Ingrese una URL válida.")
    else:
        if not st.session_state.repo_info:
            st.session_state.repo_info = get_repo_info(oai_url)
        repo_info = st.session_state.repo_info
        
        if repo_info:
            # Info Header
            with st.expander("ℹ️ Información Técnica del Servidor", expanded=False):
                c1, c2 = st.columns(2)
                c1.write(f"**Nombre:** {repo_info['Nombre']}")
                c2.write(f"**ID:** {repo_info.get('Repository ID')}")

            # Cosecha
            df_raw = harvest_dynamic(oai_url, limit)

            if not df_raw.empty:
                # --- PRE-PROCESAMIENTO DE DATOS ---
                df = df_raw.copy()

                # 1. Extraer Año
                date_col = 'date' if 'date' in df.columns else None
                if date_col:
                    def extract_year(d):
                        match = re.search(r'\d{4}', str(d))
                        return match.group(0) if match else "Sin Año"
                    df['year_extracted'] = df[date_col].apply(extract_year)
                else:
                    df['year_extracted'] = "No Data"

                # 2. Extraer Formato Limpio (PDF/XML/etc)
                if 'format' in df.columns:
                    df['clean_format'] = df['format'].apply(detect_clean_format)
                else:
                    df['clean_format'] = "Sin Formato"

                # 3. Limpiar Tipos para el Filtro (tomar el primero si hay múltiples)
                if 'type' in df.columns:
                    df['primary_type'] = df['type'].apply(lambda x: str(x).split(';')[0] if x else "Desconocido")
                else:
                    df['primary_type'] = "Desconocido"

                # --- FILTROS DINÁMICOS (SIDEBAR) ---
                st.sidebar.divider()
                st.sidebar.header("3. Filtros de Visualización")
                
                # Filtro Años
                available_years = sorted(list(df['year_extracted'].unique()))
                sel_years = st.sidebar.multiselect("Filtrar por Año", available_years, default=available_years)
                
                # Filtro Tipos
                available_types = sorted(list(df['primary_type'].unique()))
                sel_types = st.sidebar.multiselect("Filtrar por Tipo", available_types, default=available_types)

                # APLICAR FILTROS
                if sel_years:
                    df = df[df['year_extracted'].isin(sel_years)]
                if sel_types:
                    # El filtro de tipo es aproximado porque un registro puede tener varios tipos
                    # Aquí filtramos si el string contiene alguno de los seleccionados
                    pattern = '|'.join([re.escape(t) for t in sel_types])
                    if pattern:
                        df = df[df['type'].astype(str).str.contains(pattern, na=False)]

                st.success(f"Visualizando {len(df)} registros filtrados de un total de {len(df_raw)} cosechados.")

                # --- DASHBOARD ---
                
                # 1. KPIs
                st.subheader("Indicadores Clave de Rendimiento (KPIs)")
                k1, k2, k3, k4 = st.columns(4)
                
                k1.metric("Total Muestra", len(df))
                
                missing_title = df['title'].isnull().sum() if 'title' in df.columns else len(df)
                k2.metric("Sin Título", missing_title, delta_color="inverse")
                
                missing_desc = df['description'].isnull().sum() if 'description' in df.columns else len(df)
                k3.metric("Sin Descripción", missing_desc, delta_color="inverse")
                
                # Contamos cuántos NO son PDF/Word/XML/Imagen
                unknown_fmt = df[df['clean_format'] == 'Otros/Desconocido'].shape[0]
                k4.metric("Formatos Ambiguos", unknown_fmt, delta_color="inverse", help="Registros donde no se pudo detectar PDF, XML, Imagen, etc.")

                st.divider()

                # TABS
                tab1, tab2, tab3, tab4 = st.tabs([
                    "Evolución Temporal", 
                    "Tipologías y Formatos", 
                    "Análisis de Completitud",
                    "Volumen de Metadatos"
                ])

                # TAB 1: TIEMPO
                with tab1:
                    if 'year_extracted' in df.columns:
                        year_counts = df['year_extracted'].value_counts().sort_index().reset_index()
                        year_counts.columns = ['Año', 'Cantidad']
                        fig_date = px.bar(year_counts, x='Año', y='Cantidad', title="Ingresos por Año")
                        st.plotly_chart(fig_date, use_container_width=True)

                # TAB 2: TIPOLOGÍAS (LAYOUT 3 COLUMNAS)
                with tab2:
                    col_t1, col_t2, col_t3 = st.columns(3)
                    
                    with col_t1:
                        st.markdown("**Tipología Documental**")
                        if 'type' in df.columns:
                            type_data = split_and_count(df, 'type')
                            if not type_data.empty:
                                fig_type = px.pie(type_data, names='Valor', values='Frecuencia', hole=0.5)
                                fig_type.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0))
                                st.plotly_chart(fig_type, use_container_width=True)
                            else:
                                st.caption("Datos no disponibles")

                    with col_t2:
                        st.markdown("**Idiomas (ISO)**")
                        if 'language' in df.columns:
                            lang_data = split_and_count(df, 'language')
                            if not lang_data.empty:
                                fig_lang = px.pie(lang_data, names='Valor', values='Frecuencia', hole=0.5)
                                fig_lang.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0))
                                st.plotly_chart(fig_lang, use_container_width=True)
                            else:
                                st.caption("Datos no disponibles")

                    with col_t3:
                        st.markdown("**Formatos de Archivo (Detectados)**")
                        # Usamos la columna limpia 'clean_format'
                        fmt_counts = df['clean_format'].value_counts().reset_index()
                        fmt_counts.columns = ['Formato', 'Cantidad']
                        if not fmt_counts.empty:
                            fig_fmt = px.pie(fmt_counts, names='Formato', values='Cantidad', hole=0.5)
                            fig_fmt.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0))
                            st.plotly_chart(fig_fmt, use_container_width=True)
                        else:
                            st.caption("Datos no disponibles")

                # TAB 3: COMPLETITUD (SEMÁFORO)
                with tab3:
                    st.markdown("##### Nivel de Completitud de Metadatos (Semáforo)")
                    
                    cols_to_exclude = ['identifier', 'datestamp', 'count_creators', 'count_subjects', 'year_extracted', 'clean_format', 'primary_type']
                    meta_cols = [c for c in df.columns if c not in cols_to_exclude]
                    
                    # Calcular %
                    comp = df[meta_cols].notnull().mean().mul(100)
                    
                    # Clasificar
                    red_fields = comp[comp < 80].sort_values()
                    yellow_fields = comp[(comp >= 80) & (comp < 100)].sort_values()
                    green_fields = comp[comp == 100].sort_values()
                    
                    c_red, c_yellow, c_green = st.columns(3)
                    
                    def make_bar(series, color, title):
                        if series.empty:
                            return None
                        fig = go.Figure(go.Bar(
                            x=series.values, y=series.index, orientation='h',
                            marker_color=color, text=[f"{v:.1f}%" for v in series.values], textposition='auto'
                        ))
                        fig.update_layout(title=title, xaxis=dict(range=[0, 105]), height=300, margin=dict(l=0,r=0,t=40,b=0))
                        return fig

                    with c_red:
                        fig_r = make_bar(red_fields, '#FF4B4B', '🔴 Críticos (<80%)')
                        if fig_r: st.plotly_chart(fig_r, use_container_width=True)
                        else: st.success("Sin campos críticos.")

                    with c_yellow:
                        fig_y = make_bar(yellow_fields, '#FFAA00', '🟡 Aceptables (80-99%)')
                        if fig_y: st.plotly_chart(fig_y, use_container_width=True)
                        else: st.info("Sin campos en alerta.")

                    with c_green:
                        fig_g = make_bar(green_fields, '#09AB3B', '🟢 Óptimos (100%)')
                        if fig_g: st.plotly_chart(fig_g, use_container_width=True)
                        else: st.info("Ningún campo al 100%.")

                # TAB 4: VOLUMEN
                with tab4:
                    c_v1, c_v2 = st.columns(2)
                    with c_v1:
                        fig_auth = px.histogram(df, x="count_creators", nbins=20, title="Distribución: Autores por Ítem")
                        st.plotly_chart(fig_auth, use_container_width=True)
                    with c_v2:
                        fig_sub = px.histogram(df, x="count_subjects", nbins=20, title="Distribución: Materias por Ítem")
                        st.plotly_chart(fig_sub, use_container_width=True)

                # --- 4. EXPLORADOR DE DATOS ---
                st.divider()
                st.subheader("Explorador de Registros")
                
                page_size = 50
                total_items = len(df)
                total_pages = math.ceil(total_items / page_size)
                
                col_pag1, col_pag2 = st.columns([1, 4])
                with col_pag1:
                    page_number = st.number_input("Página", min_value=1, max_value=max(1, total_pages), value=1)
                with col_pag2:
                    st.write(f"Mostrando {page_size} registros por página. Total: {total_items}.")

                start_idx = (page_number - 1) * page_size
                end_idx = start_idx + page_size
                
                st.dataframe(df.iloc[start_idx:end_idx])
                
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button("⬇️ Descargar Datos Filtrados (CSV)", data=csv, file_name="auditoria_filtrada.csv", mime="text/csv")

        else:
            st.error("No se pudo conectar al repositorio.")