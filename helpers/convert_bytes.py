import base58

def convert_bytes(value, encoding='hex'):
    if encoding == 'base58':
        return base58.b58encode(value).decode()
    else:
        return value.hex()

