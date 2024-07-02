import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass

@dataclass
class GetPageNumber:
    
    _category: str = 'filmes'
    
    def get_page_number(self):
        '''
            Extract the last page number of a given category
        '''
        
        url = f'https://superfilmes.red/{self._category}/1/'

        try:
            r = requests.get(
                url = url,
                headers = {'encoding':'utf-8'}
            )

            max_page = max(
                [
                    int(i.text) 
                    for i in BeautifulSoup(r.text, 'html.parser').find('ul','pagination').find_all('a') 
                    if i.text != ''
                ]
            )

            return max_page
            
        except Exception as e:
            return f'Erro: {e}'
        