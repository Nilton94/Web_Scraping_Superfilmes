import streamlit as st
import asyncio
import pandas as pd
import datetime, pytz
from utils.utils_movies_metadata import GetMovieMetadata
from utils.utils_series_metadata import GetSeriesMetadata

def get_config():

    # CONFIGS
    st.set_page_config(
        page_title = "Filmes e Séries Disponíveis no Superfilmes",
        layout = 'wide',
        menu_items = {
            'about': 'App simples, com objetivos educativos, para obter lista de filmes e séries disponíveis no site superfilmes. Criado por josenilton1878@gmail.com'
        }

    )

def get_widgets():
    
    # WIDGETS
    st.sidebar.image(
        image = 'https://variety.com/wp-content/uploads/2023/04/movie-theater-placeholder.jpg?w=500'
    )

    st.sidebar.markdown('---')

    st.sidebar.selectbox(
        label = 'Categoria',
        options = ['series', 'filmes'],
        key = 'categorias'
    )

    st.sidebar.button(
        label = 'Atualizar',
        key = 'atualizar',
        use_container_width = True
    )

    if 'dataframe_base' not in st.session_state:
        st.session_state['dataframe_base'] = []

    if 'dataframe_filtro' not in st.session_state:
        st.session_state['dataframe_filtro'] = []

# async def get_data():
    
#     dados = await asyncio.gather(GetMovieMetadata().get_movie_metadata(), GetSeriesMetadata().get_series_metadata())
#     return dados

def get_data():
    
    # BASE
    dados_r = st.session_state['dataframe_base']

    try:
        categorias = {}
        for i in range(0, len(dados_r)):
            df = pd.DataFrame(dados_r[i][0])
            categorias.update(
                {
                    list(df.categoria.unique())[0]:i
                }
            )
    except:
        categorias = {}
    
    if st.session_state.atualizar and st.session_state.categorias not in list(categorias.keys()):
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

        st.session_state['dataframe_base'].append(df_base.to_dict(orient = 'list'))
    else:
        df_base = pd.DataFrame(dados_r[categorias[st.session_state.categorias]][0])
        st.session_state['dataframe_base'].append(df_base.to_dict(orient = 'list'))

def get_filters(df):
    
    # FILTROS
    if st.session_state.categorias == 'series':
        
        c1, c2 = st.columns([.85,.15])
        with c1:
            with st.container(border = True):
                col1, col2, col3, col4 = st.columns(4)
                
                col1.multiselect(
                    label = '**Gênero**',
                    options = df.genero.unique(),
                    key = 'genero'
                )
                col2.slider(
                    label = '**Ano de Lançamento**',
                    # options = df.ano_raw.sort_values().unique(),
                    min_value = int(df.ano_lancamento.min()),
                    max_value = int(df.ano_lancamento.max()),
                    value = (int(df.ano_lancamento.min()), int(df.ano_lancamento.max())),
                    step = 1,
                    key = 'ano'
                )
                col3.slider(
                    label = '**Temporadas**',
                    # options = df.temporadas.sort_values().unique(),
                    min_value = int(df.temporadas.min()),
                    max_value = int(df.temporadas.max()),
                    value = (int(df.temporadas.min()), int(df.temporadas.max())),
                    step = 1,
                    key = 'temporadas'
                )
                col4.slider(
                    label = '**Episódios**',
                    # options = df.episodios.sort_values().unique(),
                    min_value = int(df.episodios.min()),
                    max_value = int(df.episodios.max()),
                    value = (int(df.episodios.min()), int(df.episodios.max())),
                    step = 1,
                    key = 'episodios'
                )

        with c2:
            with st.container(border = True):
                st.markdown('**Aplicar Filtros**')
                st.button(
                    label = '**Filtrar**',
                    type = 'primary',
                    key = 'filtrar',
                    use_container_width = True
                )
    else:
        
        c1, c2 = st.columns([.85,.15])
        with c1:
            with st.container(border = True):
                col1, col2, col3, col4 = st.columns(4)

                col1.multiselect(
                    label = '**Gênero**',
                    options = df.genero.unique(),
                    key = 'genero'
                )
                col2.slider(
                    label = '**Ano de Lançamento**',
                    # options = df.ano_raw.sort_values().unique(),
                    min_value = int(df.ano_lancamento.min()),
                    max_value = int(df.ano_lancamento.max()),
                    value = (int(df.ano_lancamento.min()), int(df.ano_lancamento.max())),
                    step = 1,
                    key = 'ano'
                )
                col3.slider(
                    label = '**IMDB**',
                    # options = df.nota_float.sort_values().unique(),
                    min_value = float(df.nota_float.min()),
                    max_value = float(df.nota_float.max()),
                    value = (float(df.nota_float.min()), float(df.nota_float.max())),
                    step = 0.1,
                    key = 'nota_float'
                )
                col4.multiselect(
                    label = '**Legendas**',
                    options = df.legendas.sort_values().unique(),
                    key = 'legendas'
                )
        with c2:
            with st.container(border = True):
                st.markdown('**Aplicar Filtros**')
                st.button(
                    label = '**Filtrar**',
                    type = 'primary',
                    key = 'filtrar',
                    use_container_width = True
                )

def apply_filters(df_base_st):
    
    
    genero = df_base_st.genero.unique() if len(st.session_state.genero) == 0 else st.session_state.genero

    if st.session_state.filtrar:
        if st.session_state.categorias == 'series':
            st.session_state['dataframe_filtro'].insert(
                0,
                df_base_st
                .loc[
                    (df_base_st.genero.isin(genero))
                    & (df_base_st.ano_lancamento.between(st.session_state.ano[0], st.session_state.ano[1]))
                    & (df_base_st.temporadas.between(st.session_state.temporadas[0], st.session_state.temporadas[1]))
                    & (df_base_st.episodios.between(st.session_state.episodios[0], st.session_state.episodios[1]))
                ]
                .to_dict(orient = 'list')
            )
            # df_base_st_filt = pd.DataFrame(st.session_state['dataframe_filtro'][-1])

        else:
            legendas = df_base_st.legendas.unique() if len(st.session_state.legendas) == 0 else st.session_state.legendas

            st.session_state['dataframe_filtro'].insert(
                0,
                df_base_st
                .loc[
                    (df_base_st.genero.isin(genero))
                    & (df_base_st.ano_lancamento.between(st.session_state.ano[0], st.session_state.ano[1]))
                    & (
                        df_base_st.nota_float.between(
                            st.session_state.nota_float[0], 
                            st.session_state.nota_float[1]
                        )
                    )
                    & (df_base_st.legendas.isin(legendas))
                ]
                .to_dict(orient = 'list')
            )
            # df_base_st_filt = pd.DataFrame(st.session_state['dataframe_filtro'][-1])
    else:
        # df_base_st_filt = df_base_st.copy()
        if len(st.session_state['dataframe_filtro']) > 0:
           pass 
        else:
            st.session_state['dataframe_filtro'].insert(0, df_base_st.to_dict(orient = 'list'))
    
    return pd.DataFrame(st.session_state['dataframe_filtro'][0])

   

def get_final_dataframe(df_base_st_filt):
    
    if st.session_state.categorias == 'series':
        df_final = df_base_st_filt[['image', 'name', 'url', 'description', 'genero', 'episodios', 'temporadas', 'ano_lancamento', 'data_extracao']].sort_values('ano_lancamento', ascending = False, ignore_index = True)
    else:
        df_final = df_base_st_filt[['image', 'name', 'url', 'description', 'genero', 'nota_float', 'legendas', 'ano_lancamento', 'data_extracao']].sort_values('nota_float', ascending = False, ignore_index = True)

    st.data_editor(
        data = df_final, 
        use_container_width = True,
        column_config = {
            'url': st.column_config.LinkColumn(
                label = 'url',
                disabled = True,
                display_text = 'Link'
            ),
            'image': st.column_config.ImageColumn(
                label = 'logo',
                width = 'medium'
            )
        },
        height = 600,
        hide_index = True
    )