from utils.utils_movies_metadata import GetMovieMetadata
from utils.utils_series_metadata import GetSeriesMetadata
import asyncio
import datetime, pytz
import pandas as pd
import streamlit as st 
from IPython.display import display, HTML
from utils.utils_streamlit import get_config, get_widgets, get_data, get_filters, apply_filters, get_final_dataframe

# CONFIGS
get_config()

# WIDGETS
get_widgets()

# BASE
if st.session_state.atualizar:
    # DADOS
    inicio = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond = 0, tzinfo = None)

    if st.session_state.categorias == 'series':
        dados = asyncio.run(GetSeriesMetadata(st.session_state.categorias).get_series_metadata())
    else:
        dados = asyncio.run(GetMovieMetadata(st.session_state.categorias).get_movie_metadata())

    fim = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond = 0, tzinfo = None)
    
    # SIDEBAR COM DURACAO
    st.sidebar.markdown('---')
    try:
        st.sidebar.write(f"<b>Início</b>: {inicio}", unsafe_allow_html = True)
        st.sidebar.write(f"<b>Fim</b>: {fim}", unsafe_allow_html = True)
        st.sidebar.write(f"<b>Duração</b>: {fim - inicio}", unsafe_allow_html = True)
    except:
        pass

    df_base = pd.DataFrame(dados[0])
    # st.session_state['dataframe_base'].append(df_base.to_dict(orient = 'list'))
    st.session_state['dataframe_base'].insert(0, df_base.to_dict(orient = 'list'))

# FILTROS
# try:
if not st.session_state.atualizar:
    pass
else:
    if st.session_state.atualizar or len(st.session_state['dataframe_base']) > 0:

        df_base_st = pd.DataFrame(st.session_state['dataframe_base'][0])

        get_filters(df_base_st)

        df_filtered = apply_filters(df_base_st)

        get_final_dataframe(df_base_st_filt = df_filtered)

    else:
        st.error('Selecione uma categoria!')
# except:
#     st.write('Selecione uma categoria!')