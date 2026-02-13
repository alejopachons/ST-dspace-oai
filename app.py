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

# --- GESTIÓN DE ESTADO (SESSION STATE) ---
if 'repo_info' not in st.session_state:
    st.session_state.repo_info = None
if 'harvested_df' not in st.session_state:
    st.session_state.harvested_df = None

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
    """Deduce formato real (PDF, XML, etc)"""
    if not format_list_str or pd.isna(format_list_str):
        return "[ SIN DATO ]"
    
    text = str(format_list_str).lower()
    
    if 'pdf' in text or 'application/pdf' in text: return 'PDF'
    if 'xml' in text: return 'XML'
    if any(x in text for x in ['jpg', 'jpeg', 'png', 'gif', 'image']): return 'Imagen'
    if 'word' in text or 'doc' in text or 'docx' in text: return 'Word'
    if 'excel' in text or 'xls' in text: return 'Excel'
    if 'zip' in text or 'rar' in text: return 'Archivo Comprimido'
    if 'mp4' in text or 'video' in text: return 'Video'
    if 'mp3' in text or 'audio' in text: return 'Audio'
    
    return 'Otros/Desconocido'

def extract_year_func(d):
    """
    Extrae un año válido (1900-2099) de una cadena sucia.
    Evita capturar IDs numéricos o nombres de archivo.
    """
    if not d or pd.isna(d):
        return "[ SIN DATO ]"
    
    text = str(d)
    # Regex Explicación:
    # \b        -> Límite de palabra (evita capturar 2017 dentro de 412230132017)
    # (?:19|20) -> El año debe empezar por 19 o 20
    # \d{2}     -> Seguido de dos dígitos cualquiera
    # \b        -> Límite de palabra final
    pattern = r'\b(?:19|20)\d{2}\b'
    
    matches = re.findall(pattern, text)
    
    if matches:
        # Si encuentra varios (ej: 2023 timestamp y 2017 fecha), 
        # tomamos el primero o el más antiguo según preferencia. 
        # Normalmente el primero válido suele ser el mejor candidato en DSpace.
        return matches[0]
        
    return "[ SIN DATO ]"

def clean_split_type(type_str):
    """Limpia agresivamente el tipo documental"""
    if not type_str or pd.isna(type_str):
        return None
    
    items = [i.strip() for i in str(type_str).split(';')]
    valid_items = []
    for i in items:
        if i.startswith("info:eu-repo") or i.startswith("http") or i.startswith("puerl") or len(i) < 2:
            continue
        valid_items.append(i.title())
        
    if not valid_items:
        return None
    return valid_items[0]

def split_and_count_clean(df, column, top_n=20):
    """Cuenta valores ignorando basura técnica"""
    if column not in df.columns:
        return pd.DataFrame()
    
    all_items = []
    for item_str in df[column].dropna():
        items = [i.strip() for i in str(item_str).split(';')]
        for i in items:
            if not i.startswith("info:eu-repo") and not i.startswith("http") and not i.startswith("Driver"):
                if i != "[ SIN DATO ]":
                    all_items.append(i)
    
    if not all_items:
        return pd.DataFrame()
        
    counts = pd.DataFrame(Counter(all_items).most_common(top_n), columns=['Valor', 'Frecuencia'])
    return counts

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

# Botón de Inicio
if st.sidebar.button("🚀 Iniciar Auditoría", type="primary"):
    if not oai_url:
        st.warning("Ingrese una URL válida.")
    else:
        if not st.session_state.repo_info:
            st.session_state.repo_info = get_repo_info(oai_url)
        
        if st.session_state.repo_info:
            df_raw = harvest_dynamic(oai_url, limit)
            if not df_raw.empty:
                st.session_state.harvested_df = df_raw
            else:
                st.error("La cosecha no devolvió registros.")

# --- RENDERIZADO DEL DASHBOARD ---
if st.session_state.repo_info and st.session_state.harvested_df is not None:
    
    repo_info = st.session_state.repo_info
    df_full = st.session_state.harvested_df.copy()

    # --- PRE-PROCESAMIENTO ---
    
    # 1. Año (Lógica corregida)
    if 'year_extracted' not in df_full.columns:
        date_col = 'date' if 'date' in df_full.columns else None
        if date_col:
            df_full['year_extracted'] = df_full[date_col].apply(extract_year_func)
        else:
            df_full['year_extracted'] = "[ SIN DATO ]"
    
    # 2. Formato
    if 'clean_format' not in df_full.columns:
        if 'format' in df_full.columns:
            df_full['clean_format'] = df_full['format'].apply(detect_clean_format)
        else:
            df_full['clean_format'] = "[ SIN DATO ]"

    # 3. Tipo Principal
    if 'primary_type' not in df_full.columns:
        if 'type' in df_full.columns:
            df_full['primary_type'] = df_full['type'].apply(clean_split_type).fillna("[ SIN DATO ]")
        else:
            df_full['primary_type'] = "[ SIN DATO ]"
    
    # 4. Idioma Principal
    if 'primary_lang' not in df_full.columns:
        if 'language' in df_full.columns:
            df_full['primary_lang'] = df_full['language'].apply(lambda x: str(x).split(';')[0] if x else "[ SIN DATO ]")
        else:
            df_full['primary_lang'] = "[ SIN DATO ]"

    # 5. Derechos
    if 'rights' not in df_full.columns:
        df_full['rights'] = None

    # Info Header
    with st.expander("ℹ️ Información Técnica del Servidor", expanded=False):
        c1, c2 = st.columns(2)
        c1.write(f"**Nombre:** {repo_info['Nombre']}")
        c2.write(f"**ID:** {repo_info.get('Repository ID')}")

    # --- SECCIÓN DE FILTROS ---
    st.divider()
    st.subheader("🔍 Filtros de Visualización")
    
    with st.container(border=True):
        # FILA 1: DIMENSIONES
        c_f1, c_f2, c_f3, c_f4 = st.columns(4)
        
        # Filtros estándar (sin checkbox "Todos")
        available_years = sorted(list(df_full['year_extracted'].unique()))
        sel_years = c_f1.multiselect("Año de Publicación", available_years, default=available_years)

        available_types = sorted(list(df_full['primary_type'].unique()))
        sel_types = c_f2.multiselect("Tipología", available_types, default=available_types)

        available_langs = sorted(list(df_full['primary_lang'].unique()))
        sel_langs = c_f3.multiselect("Idioma", available_langs, default=available_langs)

        available_formats = sorted(list(df_full['clean_format'].unique()))
        sel_formats = c_f4.multiselect("Formato Detectado", available_formats, default=available_formats)

        # FILA 2: BOOLEANOS
        st.write("") 
        st.markdown("**Control de Calidad:**")
        
        c_b1, c_b2, c_b3 = st.columns(3)
        with c_b1:
            filter_empty_desc = st.checkbox("⚠️ Mostrar solo registros SIN Descripción")
        with c_b2:
            filter_no_rights = st.checkbox("⚠️ Mostrar solo registros SIN Licencia (Rights)")

    # --- APLICACIÓN LÓGICA ---
    df = df_full.copy()

    # Si hay selección, filtramos. Si está vacío, asumimos "Todo" para no dejar la pantalla en blanco.
    if sel_years:
        df = df[df['year_extracted'].isin(sel_years)]
    if sel_types:
        df = df[df['primary_type'].isin(sel_types)]
    if sel_langs:
        df = df[df['primary_lang'].isin(sel_langs)]
    if sel_formats:
        df = df[df['clean_format'].isin(sel_formats)]

    if filter_empty_desc:
        df = df[df['description'].isnull() | (df['description'].astype(str).str.strip() == "")]
    
    if filter_no_rights:
        df = df[df['rights'].isnull() | (df['rights'].astype(str).str.strip() == "")]

    # --- VISUALIZACIÓN ---
    if len(df) == 0:
        st.warning("⚠️ Los filtros seleccionados no produjeron resultados.")
    else:
        st.success(f"Visualizando registros filtrados.")

        # 1. KPIs
        st.subheader("Indicadores Clave de Rendimiento (KPIs)")
        k1, k2, k3, k4 = st.columns(4)
        
        # KPI 1: TOTAL ABSOLUTO (Estático)
        k1.metric("Total Cosechado (Base)", len(df_full), help="Cantidad total de registros descargados inicialmente.")
        
        # KPI 2: MUESTRA FILTRADA (Dinámico)
        k2.metric("Visualizando Ahora", len(df), help="Cantidad de registros según los filtros aplicados.")
        
        # KPI 3: Rights
        missing_rights = df['rights'].isnull().sum()
        k3.metric("Sin Licencia (Rights)", missing_rights, delta_color="inverse")
        
        # KPI 4: Descripción
        missing_desc = df['description'].isnull().sum() if 'description' in df.columns else len(df)
        k4.metric("Sin Descripción", missing_desc, delta_color="inverse")
        
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
            if 'year_extracted' in df.columns and not df.empty:
                df_time = df[df['year_extracted'] != "[ SIN DATO ]"]
                year_counts = df_time['year_extracted'].value_counts().sort_index().reset_index()
                year_counts.columns = ['Año', 'Cantidad']
                fig_date = px.bar(year_counts, x='Año', y='Cantidad', title="Ingresos por Año")
                st.plotly_chart(fig_date, use_container_width=True)
            else:
                st.info("No hay datos de fecha válidos.")

        # TAB 2: TIPOLOGÍAS
        with tab2:
            col_t1, col_t2, col_t3 = st.columns(3)
            
            def plot_bar_h(data, x_col, y_col, color_seq):
                fig = px.bar(data, x=x_col, y=y_col, orientation='h', text=x_col, color_discrete_sequence=[color_seq])
                fig.update_layout(showlegend=False, xaxis_title=None, yaxis_title=None, height=350, margin=dict(l=0, r=0, t=30, b=0))
                return fig

            with col_t1:
                st.markdown("**Tipología Documental (Limpia)**")
                if 'type' in df.columns and not df.empty:
                    type_data = split_and_count_clean(df, 'type')
                    if not type_data.empty:
                        fig_type = plot_bar_h(type_data.head(10), 'Frecuencia', 'Valor', '#636EFA')
                        st.plotly_chart(fig_type, use_container_width=True)
            
            with col_t2:
                st.markdown("**Idiomas (ISO)**")
                if 'language' in df.columns and not df.empty:
                    lang_data = split_and_count_clean(df, 'language')
                    if not lang_data.empty:
                        fig_lang = plot_bar_h(lang_data.head(10), 'Frecuencia', 'Valor', '#EF553B')
                        st.plotly_chart(fig_lang, use_container_width=True)

            with col_t3:
                st.markdown("**Formatos (Detectados)**")
                if 'clean_format' in df.columns and not df.empty:
                    fmt_counts = df['clean_format'].value_counts().reset_index()
                    fmt_counts.columns = ['Valor', 'Frecuencia']
                    if not fmt_counts.empty:
                        fig_fmt = plot_bar_h(fmt_counts.head(10), 'Frecuencia', 'Valor', '#00CC96')
                        st.plotly_chart(fig_fmt, use_container_width=True)

        # TAB 3: COMPLETITUD
        with tab3:
            st.markdown("##### Nivel de Completitud de Metadatos (Semáforo)")
            cols_to_exclude = ['identifier', 'datestamp', 'count_creators', 'count_subjects', 'year_extracted', 'clean_format', 'primary_type', 'primary_lang', 'rights']
            meta_cols = [c for c in df.columns if c not in cols_to_exclude]
            
            if not df.empty:
                comp = df[meta_cols].notnull().mean().mul(100)
                
                red_fields = comp[comp < 80].sort_values()
                yellow_fields = comp[(comp >= 80) & (comp < 100)].sort_values()
                green_fields = comp[comp == 100].sort_values()
                
                c_red, c_yellow, c_green = st.columns(3)
                
                def make_bar_sem(series, color, title):
                    if series.empty: return None
                    fig = go.Figure(go.Bar(
                        x=series.values, y=series.index, orientation='h',
                        marker_color=color, text=[f"{v:.1f}%" for v in series.values], textposition='auto'
                    ))
                    fig.update_layout(title=title, xaxis=dict(range=[0, 105]), height=300, margin=dict(l=0,r=0,t=40,b=0))
                    return fig

                with c_red:
                    fig_r = make_bar_sem(red_fields, '#FF4B4B', '🔴 Críticos (<80%)')
                    if fig_r: st.plotly_chart(fig_r, use_container_width=True)
                    else: st.success("Sin campos críticos.")

                with c_yellow:
                    fig_y = make_bar_sem(yellow_fields, '#FFAA00', '🟡 Aceptables (80-99%)')
                    if fig_y: st.plotly_chart(fig_y, use_container_width=True)
                    else: st.info("Sin campos en alerta.")

                with c_green:
                    fig_g = make_bar_sem(green_fields, '#09AB3B', '🟢 Óptimos (100%)')
                    if fig_g: st.plotly_chart(fig_g, use_container_width=True)
                    else: st.info("Ningún campo al 100%.")

        # TAB 4: VOLUMEN
        with tab4:
            if not df.empty:
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
        
        c_page_size, c_pagination_info = st.columns([1, 4])
        with c_page_size:
            page_size = st.selectbox("Registros por página", [10, 50, 250, 500], index=1)
        
        total_items = len(df)
        total_pages = math.ceil(total_items / page_size)
        
        if total_pages > 0:
            with c_pagination_info:
                st.write("")
                page_number = st.number_input(f"Ir a Página (1 - {total_pages})", min_value=1, max_value=max(1, total_pages), value=1)
                st.caption(f"Mostrando {page_size} de {total_items} registros.")

            start_idx = (page_number - 1) * page_size
            end_idx = start_idx + page_size
            
            st.dataframe(df.iloc[start_idx:end_idx])
            
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button("⬇️ Descargar Datos Filtrados (CSV)", data=csv, file_name="auditoria_filtrada.csv", mime="text/csv")
        else:
            st.info("No hay datos para mostrar en la tabla.")