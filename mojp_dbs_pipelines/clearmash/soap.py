from zeep import Client
from zeep.cache import SqliteCache
from zeep.transports import Transport
from .constants import WEB_CONTENT_MANAGEMENT_WSDL, AUTHENTICATION_WSDL
from zeep.plugins import HistoryPlugin
from ..settings import CLEARMASH_CLIENT_TOKEN
from zeep import xsd

history = HistoryPlugin()
transport = Transport(cache=SqliteCache())

def _get_client_token_header():
    header = xsd.Element('ClientToken', xsd.ComplexType([xsd.Element('ClientToken', xsd.String())]))
    return header()

def _get_client_plugins():
    return []
    return [history]

def _get_client(wsdl):
    client = Client(wsdl, transport=transport, plugins=_get_client_plugins())
    client.set_default_soapheaders(_get_client_token_header())
    return client

def get_auth_client():
    return _get_client(AUTHENTICATION_WSDL)

def get_wcm_client():
    return _get_client(WEB_CONTENT_MANAGEMENT_WSDL)
