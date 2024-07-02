import requests
import os
import base64

def save_image(uuid, category, logo_url):
    
    try:
        path = os.path.join(os.getcwd(), 'assets', f'{category}', f'{uuid}.png') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'assets', f'{category}', f'{uuid}.png')
        
        if os.path.isfile(path):
            pass
        else:
            r = requests.get(logo_url)

            with open(path, "wb") as f:
                f.write(r.content)

    except Exception as e:
        print(f'Erro: {e}')


def open_image(uuid, category):
    
    try:
        path = os.path.join(os.getcwd(), 'assets', f'{category}', f'{uuid}.png') if os.getcwd().__contains__('app') else os.path.join(os.getcwd(), 'app', 'assets', f'{category}', f'{uuid}.png')

        with open(path, "rb") as p:
            file = p.read()
            return f"data:image/png;base64,{base64.b64encode(file).decode()}"
    except Exception as e:
        print(f'Erro: {e}')