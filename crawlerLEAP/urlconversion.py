from urllib.parse import urlparse
        
def encode_url_to_filename(url, extension="html"):
    print(f"encode_url_to_filename(): Encoding filename for URL: {url} with extension: {extension}")

    parsed_url = urlparse(url)
    scheme = 'https-' if parsed_url.scheme == 'https' else 'http-'
    netloc = parsed_url.netloc.replace(':', '_').replace('.', '!')
    path = parsed_url.path.replace('/', '--')
    query = parsed_url.query.replace('=', '~').replace('&', '-')

    #print(f"url_parts(): ", f"scheme={scheme} --- netloc={netloc} --- query={query}.{extension}")
    if query:
        path_query = f"{path}--q--{query}"
    else:
        path_query = path
    if len(path_query) > 255:
        path_query = hashlib.md5(path_query.encode()).hexdigest()
    filename = f"{scheme}{netloc}-{path_query}.{extension}"
    filename = filename[:255]
    print(f"encode_url_to_filename(): Encoded filename: {filename}")
    return filename


