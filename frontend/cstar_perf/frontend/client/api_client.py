import requests
import json
import ConfigParser
from cstar_perf.frontend.lib.crypto import APIKey, BadConfigFileException
from cstar_perf.frontend.lib.util import encode_unicode
from cstar_perf.frontend import CLIENT_CONFIG_PATH

### Change to True to enable verbose http logging:
if False:
    import logging
    # these two lines enable debugging at httplib level (requests->urllib3->httplib)
    # you will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
    # the only thing missing will be the response.body which is not logged.
    import httplib
    httplib.HTTPConnection.debuglevel = 1

    logging.basicConfig() # you need to initialize logging, otherwise you will not see anything from requests
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True

class APIClient(object):
    def __init__(self, server_domain):
        self.endpoint = "http://"+server_domain+"/api"
        self.session = requests.Session()
        self.login()

    def get(self, path, **kwargs):
        r = self.session.get(self.endpoint + path, **kwargs)
        body = encode_unicode(r.text)
        if r.status_code == 401:
            self.login()
            r = self.session.get(self.endpoint + path, **kwargs)
        if r.status_code == 200:
            return r.json()
        error = u'Request failed to {} - {} {}'.format(path, r, body).encode("utf-8")
        raise RuntimeError(error)

    def post(self, path, data=None, **kwargs):
        kwargs['headers'] = kwargs.get('headers', {})
        kwargs['headers'].update({'content-type': 'application/json'})
        r = self.session.post(self.endpoint + path, data, **kwargs)
        body = encode_unicode(r.text)
        if r.status_code == 401 and not path.startswith('/login'):
            self.login()
            r = self.session.post(self.endpoint + path, data, **kwargs)
        elif r.status_code == 200:
            return r.json()
        error = u'Request failed to {} - {} {}'.format(path, r, body).encode("utf-8")
        raise RuntimeError(error)

    def delete(self, path, **kwargs):
        r = self.session.delete(self.endpoint + path, **kwargs)
        body = encode_unicode(r.text)
        if r.status_code == 401:
            self.login()
            r = self.session.delete(self.endpoint + path, **kwargs)
        if r.status_code == 200:
            return r.json()
        error = u'Request failed to {} - {} {}'.format(path, r, body).encode("utf-8")
        raise RuntimeError(error)

    def put(self, path, **kwargs):
        kwargs['headers'] = kwargs.get('headers', {})
        kwargs['headers'].update({'content-type': 'application/json'})
        r = self.session.put(self.endpoint + path, **kwargs)
        body = encode_unicode(r.text)
        if r.status_code == 401:
            self.login()
            r = self.session.put(self.endpoint + path, **kwargs)
        if r.status_code == 200:
            return r.json()
        error = u'Request failed to {} - {} {}'.format(path, r, body).encode("utf-8")        
        raise RuntimeError(error)
        
    def login(self):
        """Login to the server, return an authenticated requests session"""

        config = ConfigParser.RawConfigParser()
        config.read(CLIENT_CONFIG_PATH)
        self.__client_name = config.get('cluster','name')

        client_key = APIKey.load(key_type='client')
        server_key = APIKey.load(key_type='server')
        # request a login token
        r = self.get('/login')
        try:
            server_key.verify_message(r['token'], r['signature'])
        except Exception, e:
            raise RuntimeError(u'The server returned a bad signature for the token.', e)

        # Sign the token and post it back:
        data = {'login': self.__client_name,
                'signature': client_key.sign_message(r['token'])}
        r = self.post('/login', data=json.dumps(data))
        if r.get('success', '') != 'Logged in':
            raise RuntimeError(u'Login denied by server')
