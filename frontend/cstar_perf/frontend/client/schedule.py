import argparse 
import requests
import json
from cstar_perf.frontend.lib.crypto import APIKey, BadConfigFileException
from cstar_perf.frontend import CLIENT_CONFIG_PATH

class Scheduler(object):
    def __init__(self, server_domain):
        self.endpoint = "http://"+server_domain+"/api"
        self.session = requests.Session()
        self.login()

    def login(self):
        """Login to the server, return an authenticated requests session"""
        url = self.endpoint+"/login"

        config = ConfigParser.RawConfigParser()
        config.read(CLIENT_CONFIG_PATH)
        self.__client_name = config.get('cluster','name')

        client_key = APIKey.load(key_type='client')
        server_key = APIKey.load(key_type='server')
        # request a login token
        r = self.session.get(url)
        if r.status_code == 200:
            data = r.json()
            try:
                server_key.verify_message(data['token'], data['signature'])
            except Exception, e:
                raise RuntimeError('The server returned a bad signature for the token.', e)
        else:
            raise RuntimeError('Could not request login token : {} - {}'.format(r, r.text))

        # Sign the token and post it back:
        data = {'login': self.__client_name,
                'signature': client_key.sign_message(data['token'])}
        r = self.session.post(url, data=json.dumps(data), headers={'content-type': 'application/json'})
        if r.json().get('success', '') != 'Logged in':
            raise RuntimeError('Login denied by server')

    def schedule(self, job):
        """Schedule a job. job can either be a path to a file, or a dictionary"""
        if isinstance(job, basestring):
            with open(job) as f:
                job = f.read()
        else:
            job = json.dumps(job)

        print self.session.post(self.endpoint+"/tests/schedule", data=job, headers={'content-type': 'application/json'})
    

def main():
    parser = argparse.ArgumentParser(description='cstar_perf job scheduler', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-s', '--server', default='localhost:8000',
                        help='Server endpoint', dest='server')
    parser.add_argument(
        'job', help='The JSON job description file', nargs='+')
    args = parser.parse_args()

    scheduler = Scheduler(args.server)

    for job in args.job:
        scheduler.schedule(job)


if __name__ == "__main__":
    main()
