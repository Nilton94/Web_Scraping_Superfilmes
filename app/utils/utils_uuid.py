import hashlib, uuid

def generate_uuid(name):
    '''
    Cria um id único para uma dada string
    :params
        name -> str, string base para gera o id
    '''
    
    md5_hash = hashlib.md5(name.encode())
    
    return uuid.UUID(md5_hash.hexdigest())