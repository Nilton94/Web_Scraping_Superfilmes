from utils.utils_movies_metadata import GetMovieMetadata
from utils.utils_series_metadata import GetSeriesMetadata
import asyncio
import datetime, pytz
import pandas as pd
import streamlit as st 
from utils.utils_streamlit import get_config, get_widgets, get_data, get_filters, apply_filters, get_final_dataframe, get_duration

# CONFIGS
get_config()

# WIDGETS
get_widgets()

# BASE
# get_data()

# SIDEBAR COM DURACAO
# get_duration()

# FILTROS
# try:
#     if st.session_state.atualizar or (len(st.session_state['dataframe_base']) > 0 and st.session_state.categorias in list(set(st.session_state['dataframe_base'][0]['categoria']))):

#         df_base_st = pd.DataFrame(st.session_state['dataframe_base'][0])

#         get_filters(df_base_st)

#         df_filtered = apply_filters(df_base_st)

#         get_final_dataframe(df_base_st_filt = df_filtered)

#     else:
#         st.write('Selecione uma categoria!')
# except:
#     pass

# if st.session_state.limpar_cache:
#     st.cache_data.clear()

if st.session_state.atualizar:
    # DADOS
    inicio = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond = 0, tzinfo = None)
    st.session_state['lista_inicio'].insert(0, inicio) 

    if st.session_state.categorias == 'series':
        # dados = asyncio.run(GetSeriesMetadata(st.session_state.categorias).get_series_metadata())
        dados = GetSeriesMetadata(st.session_state.categorias).get_series_metadata_sync()
        # dados = await GetSeriesMetadata(st.session_state.categorias).get_series_metadata()
    else:
        # dados = asyncio.run(GetMovieMetadata(st.session_state.categorias).get_movie_metadata())
        dados = GetMovieMetadata(st.session_state.categorias).get_movie_metadata_sync()
        # dados = await GetMovieMetadata(st.session_state.categorias).get_movie_metadata()

    fim = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond = 0, tzinfo = None)
    st.session_state['lista_fim'].insert(0, fim) 

    duracao = fim - inicio
    st.session_state['lista_duracao'].insert(0, duracao)

    # df_base = pd.DataFrame(dados[0])
    df_base = pd.DataFrame(dados)
    st.session_state['dataframe_base'].insert(0, df_base.to_dict(orient = 'list'))

dados