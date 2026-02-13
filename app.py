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

# --- GESTIÓN DE ESTADO (SESSION STATE) ---
if 'repo_info' not in st.session_state:
    st.session_state.repo_info = None

# --- FUNCIONES ---

def get_repo_info(url):
    """Obtiene la identidad del repositorio"""
    try:
        sickle = Sickle(url)
        identify = sickle.Identify()
        return {
            "Nombre": getattr(identify, 'repositoryName', 'Desconocido'),
            "Base URL": getattr(identify, 'baseURL', 'Desconocido'),
            "Versión Protocolo": getattr(identify, 'protocolVersion', '2.0'),
            "Admin Email": getattr(identify, 'adminEmail', 'No público'),
            "Repository ID": getattr(identify, 'repositoryIdentifier', None) # Clave para seguridad
        }
    except Exception as e:
        return None

@st.cache_data(show_spinner=False)
def harvest_dynamic(url, limit):
    """Cosecha dinámica con manejo de errores de tipos (NoneType fix)"""
    data = []
    try:
        sickle = Sickle(url)
        iterator = sickle.ListRecords(metadataPrefix='oai_dc', ignore_deleted=True)
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, record in enumerate(iterator):
            if i >= limit:
                break
            
            # Actualizar barra cada 10 registros
            if i % 10 == 0:
                progress = min((i + 1) / limit, 1.0)
                progress_bar.progress(progress)
                status_text.text(f"Cosechando registro {i+1} de {limit}...")

            row = {
                'identifier': record.header.identifier,
                'datestamp': record.header.datestamp,
            }
            
            # --- CORRECCIÓN DE ERROR (NoneType) ---
            # Extracción dinámica segura: Filtramos Nones y convertimos todo a string
            for key, values in record.metadata.items():
                if values:
                    # Comprensión de lista para asegurar que solo unimos cadenas de texto válidas
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
    """
    Separa valores, cuenta y LIMPIA basura técnica (info:eu-repo...)
    """
    if column not in df.columns:
        return pd.DataFrame()
    
    all_items = []
    for item_str in df[column].dropna():
        # Separar por punto y coma
        items = [i.strip() for i in str(item_str).split(';')]
        for i in items:
            # FILTRO: Ignoramos las URIs técnicas de OpenAIRE/Drivers
            if not i.startswith("info:eu-repo") and not i.startswith("http"):
                all_items.append(i)
    
    if not all_items:
        return pd.DataFrame()
        
    counts = pd.DataFrame(Counter(all_items).most_common(top_n), columns=['Valor', 'Frecuencia'])
    return counts

# --- SIDEBAR: CONEXIÓN Y SEGURIDAD ---
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

# Lógica del Slider y Seguridad
limit = 500
if st.session_state.repo_info:
    repo_id = st.session_state.repo_info.get('Repository ID', 'Desconocido')
    
    st.sidebar.divider()
    st.sidebar.header("2. Configuración de Cosecha")
    
    # Checkbox para modo experto
    high_vol_mode = st.sidebar.checkbox("🔓 Habilitar Cosecha Masiva (> 5000)")
    
    if high_vol_mode:
        st.sidebar.info(f"Para desbloquear, ingrese el ID del repositorio: **{repo_id}**")
        security_check = st.sidebar.text_input("Repository Identifier")
        
        if security_check.strip() == repo_id:
            st.sidebar.success("¡Límite desbloqueado!")
            limit = st.sidebar.number_input("Cantidad de registros", min_value=5000, max_value=20000, value=5000, step=1000)
        else:
            st.sidebar.warning("El ID no coincide.")
            limit = st.sidebar.slider("Límite seguro", 100, 5000, 500)
    else:
        limit = st.sidebar.slider("Límite de registros", 100, 5000, 500)

# --- BOTÓN PRINCIPAL DE ANÁLISIS ---
run_analysis = st.sidebar.button("🚀 Iniciar Auditoría", type="primary")

# --- LÓGICA PRINCIPAL ---

if run_analysis:
    if not oai_url:
        st.warning("Ingrese una URL y verifique la conexión primero.")
    else:
        # Recuperar info si no está en sesión
        if not st.session_state.repo_info:
            st.session_state.repo_info = get_repo_info(oai_url)

        repo_info = st.session_state.repo_info
        
        if repo_info:
            # 1. MOSTRAR INFO
            with st.expander("ℹ️ Información Técnica del Servidor", expanded=True):
                c1, c2, c3 = st.columns(3)
                c1.write(f"**Nombre:** {repo_info['Nombre']}")
                c2.write(f"**ID:** {repo_info.get('Repository ID')}")
                c3.write(f"**Admin:** {repo_info['Admin Email']}")

            # 2. COSECHA
            st.divider()
            
            # Mensaje de ayuda para cancelación
            st.info("💡 Si el proceso tarda demasiado, puedes detenerlo usando el botón 'Stop' (⏹) en la esquina superior derecha de la aplicación.")
            
            st.write(f"Iniciando cosecha de **{limit}** registros...")
            
            # Llamada a la función corregida
            df = harvest_dynamic(oai_url, limit)

            if not df.empty:
                # Procesamiento Fecha
                date_col = 'date' if 'date' in df.columns else None
                if date_col:
                    def extract_year(d):
                        match = re.search(r'\d{4}', str(d))
                        return match.group(0) if match else "Sin Año"
                    df['year_extracted'] = df[date_col].apply(extract_year)

                # --- 2. RESUMEN KPI ---
                st.subheader("Signos Vitales")
                k1, k2, k3, k4 = st.columns(4)
                
                k1.metric("Registros Analizados", len(df))
                
                missing_title = df['title'].isnull().sum() if 'title' in df.columns else len(df)
                k2.metric("Sin Título (Crítico)", missing_title, delta_color="inverse")
                
                missing_desc = df['description'].isnull().sum() if 'description' in df.columns else len(df)
                k3.metric("Sin Descripción", missing_desc, delta_color="inverse")
                
                missing_date = df[df['year_extracted'] == "Sin Año"].shape[0] if 'year_extracted' in df.columns else 0
                k4.metric("Sin Fecha", missing_date, delta_color="inverse")

                # --- 3. PESTAÑAS VISUALES ---
                st.subheader("Análisis Gráfico")
                
                tab1, tab2, tab3, tab4 = st.tabs([
                    "Temporalidad", 
                    "Tipologías y Formatos", 
                    "Volumen (Autores/Materias)",
                    "Completitud (Semáforo)"
                ])

                # TAB 1: AÑOS
                with tab1:
                    if 'year_extracted' in df.columns:
                        year_counts = df['year_extracted'].value_counts().sort_index().reset_index()
                        year_counts.columns = ['Año', 'Cantidad']
                        fig_date = px.bar(year_counts, x='Año', y='Cantidad', title="Publicaciones por Año")
                        st.plotly_chart(fig_date, use_container_width=True)
                    else:
                        st.warning("No se encontraron fechas.")

                # TAB 2: TIPOS Y FORMATOS (Limpio de info:eu-repo)
                with tab2:
                    c_type, c_lang = st.columns(2)
                    
                    with c_type:
                        st.markdown("##### Tipología Documental")
                        if 'type' in df.columns:
                            type_data = split_and_count(df, 'type')
                            if not type_data.empty:
                                fig_type = px.pie(type_data, names='Valor', values='Frecuencia', hole=0.4)
                                st.plotly_chart(fig_type, use_container_width=True)
                            else:
                                st.info("No se detectaron tipos legibles (solo códigos técnicos o vacíos).")
                        else:
                            st.info("Campo 'type' vacío.")

                    with c_lang:
                        st.markdown("##### Idioma")
                        if 'language' in df.columns:
                            lang_data = split_and_count(df, 'language')
                            if not lang_data.empty:
                                fig_lang = px.pie(lang_data, names='Valor', values='Frecuencia', hole=0.4)
                                st.plotly_chart(fig_lang, use_container_width=True)
                            else:
                                st.info("No se detectaron idiomas legibles.")
                    
                    st.divider()
                    st.markdown("##### Formatos de Archivo")
                    if 'format' in df.columns:
                        fmt_data = split_and_count(df, 'format')
                        if not fmt_data.empty:
                            fig_fmt = px.bar(fmt_data, x='Frecuencia', y='Valor', orientation='h')
                            st.plotly_chart(fig_fmt, use_container_width=True)

                # TAB 3: VOLUMEN (Histogramas)
                with tab3:
                    c_v1, c_v2 = st.columns(2)
                    with c_v1:
                        fig_auth_hist = px.histogram(df, x="count_creators", nbins=20, 
                                                     title="Histograma: Cantidad de Autores por Ítem",
                                                     labels={'count_creators': 'Autores'})
                        st.plotly_chart(fig_auth_hist, use_container_width=True)
                    
                    with c_v2:
                        fig_sub_hist = px.histogram(df, x="count_subjects", nbins=20, 
                                                    title="Histograma: Cantidad de Materias por Ítem",
                                                    labels={'count_subjects': 'Materias'})
                        st.plotly_chart(fig_sub_hist, use_container_width=True)

                # TAB 4: COMPLETITUD (SEMÁFORO)
                with tab4:
                    cols_to_exclude = ['identifier', 'datestamp', 'count_creators', 'count_subjects', 'year_extracted']
                    meta_cols = [c for c in df.columns if c not in cols_to_exclude]
                    
                    completeness_series = df[meta_cols].notnull().mean().mul(100).sort_values(ascending=True)
                    
                    colors = []
                    for val in completeness_series.values:
                        if val < 80:
                            colors.append('#FF4B4B') # Rojo
                        elif val < 100:
                            colors.append('#FFAA00') # Amarillo
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
                        title="Salud de Metadatos (<80% Rojo | 80-99% Amarillo | 100% Verde)",
                        xaxis_title="% Presencia",
                        xaxis=dict(range=[0, 105])
                    )
                    st.plotly_chart(fig_comp, use_container_width=True)

                # --- 4. DATA ---
                st.subheader("Explorador de Datos")
                st.dataframe(df.head(100))
                
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "Descargar CSV Completo",
                    csv,
                    "auditoria_oai_clean.csv",
                    "text/csv"
                )

        else:
            st.error("No se pudo conectar al repositorio.")