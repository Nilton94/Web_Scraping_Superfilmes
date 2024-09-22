import streamlit as st
import asyncio
import pandas as pd
import datetime, pytz
from utils.utils_movies_metadata import GetMovieMetadata
from utils.utils_series_metadata import GetSeriesMetadata
from streamlit_card import card

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
    # st.sidebar.image(
    #     image = 'https://variety.com/wp-content/uploads/2023/04/movie-theater-placeholder.jpg?w=500'
    # )

    # st.sidebar.markdown('---')

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

    # SESSION STATE
    if 'dataframe_base' not in st.session_state:
        st.session_state['dataframe_base'] = []

    if 'dataframe_filtro' not in st.session_state:
        st.session_state['dataframe_filtro'] = []
    
    if 'lista_inicio' not in st.session_state:
        st.session_state['lista_inicio'] = []

    if 'lista_fim' not in st.session_state:
        st.session_state['lista_fim'] = []

    if 'lista_duracao' not in st.session_state:
        st.session_state['lista_duracao'] = []

def get_data():
    
    # BASE
    if st.session_state.atualizar:
        # DADOS
        inicio = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond = 0, tzinfo = None)
        st.session_state['lista_inicio'].insert(0, inicio) 

        if st.session_state.categorias == 'series':
            dados = asyncio.run(GetSeriesMetadata(st.session_state.categorias).get_series_metadata())
            # dados = GetSeriesMetadata(st.session_state.categorias).get_series_metadata_sync()
            # dados = await GetSeriesMetadata(st.session_state.categorias).get_series_metadata()
        else:
            dados = asyncio.run(GetMovieMetadata(st.session_state.categorias).get_movie_metadata())
            # dados = GetMovieMetadata(st.session_state.categorias).get_movie_metadata_sync()
            # dados = await GetMovieMetadata(st.session_state.categorias).get_movie_metadata()

        fim = datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond = 0, tzinfo = None)
        st.session_state['lista_fim'].insert(0, fim) 

        duracao = fim - inicio
        st.session_state['lista_duracao'].insert(0, duracao)

        df_base = pd.DataFrame(dados[0])
        # df_base = pd.DataFrame(dados)
        st.session_state['dataframe_base'].insert(0, df_base.to_dict(orient = 'list'))

        return dados

def get_duration():
    
    # SIDEBAR COM DURACAO
    st.sidebar.markdown('---')
    try:
        st.sidebar.write(f"<b>Início</b>: {st.session_state['lista_inicio'][0]}", unsafe_allow_html = True)
        st.sidebar.write(f"<b>Fim</b>: {st.session_state['lista_fim'][0]}", unsafe_allow_html = True)
        st.sidebar.write(f"<b>Duração</b>: {st.session_state['lista_fim'][0] - st.session_state['lista_inicio'][0]}", unsafe_allow_html = True)
    except:
        pass

def get_filters(df):
    
    
    # df = get_data()
    
    # FILTROS
    if st.session_state.categorias == 'series':
        
        c1, c2 = st.columns([.85,.15], vertical_alignment = 'center')
        with c1:
            with st.container(border = True):
                col1, col11, col2, col3, col4 = st.columns(5)
                
                col1.multiselect(
                    label = '**Nome**',
                    options = df.name.sort_values().unique(),
                    key = 'name'
                )
                col11.multiselect(
                    label = '**Gênero**',
                    options = df.genero.sort_values().unique(),
                    key = 'genero'
                )
                col2.slider(
                    label = '**Ano de Lançamento**',
                    min_value = int(df.ano_lancamento.min()),
                    max_value = int(df.ano_lancamento.max()),
                    value = (int(df.ano_lancamento.min()), int(df.ano_lancamento.max())),
                    step = 1,
                    key = 'ano'
                )
                col3.slider(
                    label = '**Temporadas**',
                    min_value = int(df.temporadas.min()),
                    max_value = int(df.temporadas.max()),
                    value = (int(df.temporadas.min()), int(df.temporadas.max())),
                    step = 1,
                    key = 'temporadas'
                )
                col4.slider(
                    label = '**Episódios**',
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
                col1, col11, col2, col3, col4 = st.columns(5)

                col1.multiselect(
                    label = '**Nome**',
                    options = df.name.sort_values().unique(),
                    key = 'name'
                )
                col11.multiselect(
                    label = '**Gênero**',
                    options = df.genero.sort_values().unique(),
                    key = 'genero'
                )
                col2.slider(
                    label = '**Ano de Lançamento**',
                    min_value = int(df.ano_lancamento.min()),
                    max_value = int(df.ano_lancamento.max()),
                    value = (int(df.ano_lancamento.min()), int(df.ano_lancamento.max())),
                    step = 1,
                    key = 'ano'
                )
                col3.slider(
                    label = '**IMDB**',
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
    name = df_base_st.name.unique() if len(st.session_state.name) == 0 else st.session_state.name

    if st.session_state.filtrar:
        if st.session_state.categorias == 'series':
            st.session_state['dataframe_filtro'].insert(
                0,
                df_base_st
                .loc[
                    (df_base_st.genero.isin(genero))
                    & (df_base_st.name.isin(name))
                    & (df_base_st.ano_lancamento.between(st.session_state.ano[0], st.session_state.ano[1]))
                    & (df_base_st.temporadas.between(st.session_state.temporadas[0], st.session_state.temporadas[1]))
                    & (df_base_st.episodios.between(st.session_state.episodios[0], st.session_state.episodios[1]))
                ]
                .to_dict(orient = 'list')
            )

        else:
            legendas = df_base_st.legendas.unique() if len(st.session_state.legendas) == 0 else st.session_state.legendas

            st.session_state['dataframe_filtro'].insert(
                0,
                df_base_st
                .loc[
                    (df_base_st.genero.isin(genero))
                    & (df_base_st.name.isin(name))
                    & (df_base_st.ano_lancamento.between(st.session_state.ano[0], st.session_state.ano[1]))
                    & (df_base_st.nota_float.between(st.session_state.nota_float[0], st.session_state.nota_float[1]))
                    & (df_base_st.legendas.isin(legendas))
                ]
                .to_dict(orient = 'list')
            )
    else:
        if len(st.session_state['dataframe_filtro']) > 0 and st.session_state.categorias in list(set(st.session_state['dataframe_filtro'][0]['categoria'])):
           pass 
        else:
            st.session_state['dataframe_filtro'].insert(0, df_base_st.to_dict(orient = 'list'))
    
    return pd.DataFrame(st.session_state['dataframe_filtro'][0])

   

def get_final_dataframe(df_base_st_filt):
    
    if st.session_state.categorias == 'series':
        df_final = df_base_st_filt[['image', 'name', 'url', 'description', 'genero', 'episodios', 'temporadas', 'ano_lancamento']].sort_values('ano_lancamento', ascending = False, ignore_index = True)
    else:
        df_final = df_base_st_filt[['image', 'name', 'url', 'description', 'genero', 'nota_float', 'legendas', 'ano_lancamento']].sort_values('nota_float', ascending = False, ignore_index = True)

    st.markdown(
        f"""
        <style>
        .card {{
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            padding: 10px;
            margin: 10px 0;
            width: 100%;
            height: 50px;
            min-height: 50px;
            background-color: "#FFFFFF";
            color: "#00008B";
            border: 1px solid #ccc;
            border-radius: 5px;
            box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.1);
            transition: all 0.3s ease-in-out;
        }}
        </style>
        <div class="card">
            <b> Resultados </b>{df_final.shape[0]}
        </div>
        """,
        unsafe_allow_html=True
    )
    
    st.dataframe(
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