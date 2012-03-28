from http.client import HTTPConnection
from urllib.parse import urlencode

def http_request(url, server, port = 80, data = None):
    """Request a URL to a HTPP server.

    `url' must be the Absolute URL in the case that `server' is the address of a proxy server.
    
    `data` is used on POST requests. `data' must be a dictionary containing the parameters that will be embedded in the
    body of the HTTP request. If `data' = None, a GET request is assumed.

    This function returns an HTTPResponse instance.
    """
    headers = {} # headers
    body  = None # formated data parameters
    method = 'GET'
    if data != None:
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        body  = urlencode(data)
        method = 'POST'

    # TODO: Timeouts!
    conn = HTTPConnection(server, port)
    print(headers, body)
    conn.request(method, url, body, headers)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return (resp.status, data)
