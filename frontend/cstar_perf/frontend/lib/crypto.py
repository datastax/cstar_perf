import os
import base64
import ConfigParser
import uuid
from distutils.dir_util import mkpath

import ecdsa

SERVER_KEY_PATH = os.path.join(os.path.expanduser("~"),'.cstar_perf', 'server.conf')

class NoPrivateKeyException(Exception):
    pass

class BadConfigFileException(Exception):
    pass


class APIKey(object):
    """Object representing the client or server api keys
    
    wraps ecdsa signing and verifying keys and returns/accepts base64 encoded messages.

    >>> apikey1 = APIKey.new()
    >>> sig1 = apikey1.sign_message('hello, world!')
    >>> apikey1.verify_message('hello, world!', sig1)
    True
    >>> apikey2 = APIKey(apikey1.get_pub_key())
    >>> apikey2.verify_message('hello, world!', sig1)
    True
    """
    def __init__(self, pubkey=None, privkey=None):
        if pubkey is None and privkey is None:
            raise AssertionError("Both privkey and pubkey are None")
        if pubkey is not None:
            self.__verifying_key = ecdsa.VerifyingKey.from_string(base64.decodestring(pubkey))
        if privkey is not None:
            self.__signing_key = ecdsa.SigningKey.from_string(base64.decodestring(privkey))
            if pubkey and self.__signing_key.get_verifying_key().to_string() != \
               self.__verifying_key.to_string():
                raise AssertionError('Public key does not match supplied Private key')
            else:
                self.__verifying_key = self.__signing_key.get_verifying_key()

    def get_pub_key(self):
        try:
            return base64.encodestring(self.__signing_key.get_verifying_key().to_string()).strip()
        except AttributeError:
            return base64.encodestring(self.__verifying_key.to_string()).strip()

    def verify_message(self, message, signature):
        signature = base64.decodestring(signature)
        return self.__verifying_key.verify(signature, message)
            
    def sign_message(self, message):
        try:
            return base64.encodestring(self.__signing_key.sign(message)).strip()
        except AttributeError:
            raise NoPrivateKeyException('This APIKey object has no private key, and cannot sign messages.')

    def save(self, config_path=None, key_type='client'):
        """Save APIKey to config file"""
        if not config_path:
            config_path = os.path.join(os.path.expanduser("~"), ".cstar_perf","client.conf")
        config = ConfigParser.RawConfigParser()
        config.read(config_path)
        if not config.has_section("credentials"):
            config.add_section("credentials")
        if key_type == 'client':
            config.set('credentials', 'priv_key', base64.encodestring(self.__signing_key.to_string()))
        elif key_type == 'server':
            config.set('credentials', 'server_key', base64.encodestring(self.__verifying_key.to_string()))
        else:
            raise AssertionError('Unknown key_type: {key_type}'.format(key_type=key_type))
        mkpath(os.path.dirname(config_path))
        with open(config_path, "wb") as f:
            config.write(f)
        os.chmod(config_path, 0600)

    @staticmethod
    def new():
        """Create new APIKey, generating a fresh ecdsa key"""
        sk = ecdsa.SigningKey.generate()
        return APIKey(privkey=base64.encodestring(sk.to_string()))
        
    @staticmethod
    def load(config_path=None, key_type='client'):
        """Load APIKey from config file

        config_path - the full path to the config file
        key_type - 'client' or 'server'"""
        
        if not config_path:
            config_path = os.path.join(os.path.expanduser("~"), ".cstar_perf", "client.conf")
        if not os.path.exists(config_path):
            raise BadConfigFileException('Config file does not exist: {config_path}'.
                                         format(config_path=config_path))
        config = ConfigParser.RawConfigParser()
        config.read(config_path)
        if key_type == 'client':
            try:
                privkey = config.get('credentials','priv_key')
            except:
                raise BadConfigFileException("Config file has no existing priv_key")
            apikey = APIKey(privkey=privkey)
        elif key_type == 'server':
            try:
                pubkey = config.get('credentials', 'server_key')
            except:
                raise BadConfigFileException("Config file has no existing server_key")
            apikey = APIKey(pubkey)

        return apikey

def generate_server_keys():
    """One time function to create and store server side keys"""
    try:
        apikey = APIKey.load(SERVER_KEY_PATH,
                             # Yes, client is correct here, because we
                             # want a signing key:
                             key_type='client')
        print("Server keys already exist in {config_path}".format(config_path=SERVER_KEY_PATH))
    except BadConfigFileException:
        apikey = APIKey.new()
        apikey.save(SERVER_KEY_PATH,
                    # Yes, client is correct here, because we are
                    # using a signing key:
                    key_type='client')
        print("New server keys saved to {config_path}".format(config_path=SERVER_KEY_PATH))

    print("Server public key: {pubkey}".format(pubkey=apikey.get_pub_key()))
    token = str(uuid.uuid1())
    sig = apikey.sign_message(token)
    verify_code = base64.b64encode(token + "|" + sig)
    print("Server verify code: {verify_code}".format(verify_code=verify_code))
