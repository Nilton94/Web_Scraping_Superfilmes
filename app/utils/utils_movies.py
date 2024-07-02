import aiohttp
import asyncio
from bs4 import BeautifulSoup
from dataclasses import dataclass
from utils.utils_uuid import generate_uuid
from utils.utils_pages import GetPageNumber
from utils.utils_images import save_image
import datetime, pytz

@dataclass
class GetMovieData:
    _category: str = 'filmes'

    async def get_urls(self, session, url):
        '''
            Returns the movie data for every url passed

            Args:
                session: aiohttp session
                url: url of the webpage that will be scraped
            Returns:
                list of dictionaries with html content
        '''

        movies = []
        async with session.get(url) as response:
            soup = BeautifulSoup(await response.text(), 'html.parser')

            for item in soup.select('.zmovo-video-item-box'):
                movie_name = item.select_one('.zmovo-v-box-content a.glow').text.strip()
                movie_url = 'https://superfilmes.red' + item.select_one('.zmovo-v-box-content a.glow')['href']
                movie_genre = item.select_one('.zmovo-v-tag span').text.strip()
                logo = 'https://superfilmes.red' + item.select_one('img')['data-src']
                
                save_image(
                    uuid = str(generate_uuid(movie_name.lower())),
                    category = self._category,
                    logo_url = logo
                )

                movies.append(
                    {
                        'uuid': str(generate_uuid(movie_name.lower())),
                        'movie_name': movie_name, 
                        'movie_url': movie_url, 
                        'movie_genre': movie_genre, 
                        'logo': logo,
                        'data_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')),
                        'ano_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).year),
                        'mes_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).month),
                        'dia_extracao': str(datetime.datetime.now(tz = pytz.timezone('America/Sao_Paulo')).replace(microsecond=0).day)
                    }
                )
        
        return movies


    async def get_movie_data(self):
        '''
            Returns all the movie data using asyncio and aiohttp

            Returns:
                list of dictionaries with html content for every movie
        '''

        try:
            page_number = GetPageNumber(_category = self._category).get_page_number()

            urls = [f'https://superfilmes.red/{self._category}/{i}/' for i in range(1, page_number+1)]
            results = []

            async with aiohttp.ClientSession(headers = {'encoding':'utf-8'}) as session:
                tasks = [
                    self.get_urls(session = session, url = x)
                    for x in urls
                ]
                html_pages = await asyncio.gather(*tasks)
                results.append(html_pages)

            movie_data = [movie for row in results[0] for movie in row]
            
            return movie_data
        
        except Exception as e:
            print(f'Erro: {e}')