from typing import List

from pydantic import BaseModel, conint


# --------- pydantic configuration schema classes ----------
class CoreConfiguration(BaseModel):
    debug: bool = False
    verbosity: conint(ge=0, lt=5) = 0
    stats: bool
    pipelines: conint(gt=0) = 2
    shipper_threads: conint(gt=0) = 1
    fetcher_threads: conint(gt=0) = 2


class DataConfiguration(BaseModel):
    file_extension: str
    ignore_field_prefixes: List[str] = []
    include: List[str] = None
    exclude: List[str] = None


class ElasticAccessConfigs(BaseModel):
    user: str = None
    password: str = None
    ask_password: bool
    enable_ssl: bool
    ssl_cert: str = None


class ElasticConnectionConfigs(BaseModel):
    max_retries: conint(ge=0) = 100
    retry_on_timeout: bool = True
    timeout: conint(gt=0) = 100


class ElasticSniffingConfigs(BaseModel):
    disable_sniffing: bool = True
    sniff_on_start: bool = False
    sniff_on_connection_fail: bool = False
    sniff_timeout: conint(gt=0) = 100


class ElasticIndexConfigs(BaseModel):
    index_template: str
    index_prefix: str = 'pydat'
    rollover_size: conint(gt=0) = 50000000


class ElasticRequestConfigs(BaseModel):
    bulk_size: conint(gt=0) = 1000
    bulk_fetch_size: conint(gt=0) = 50


class ElasticConfiguration(BaseModel):
    host: str
    access: ElasticAccessConfigs
    connection: ElasticConnectionConfigs
    sniffing: ElasticSniffingConfigs
    index: ElasticIndexConfigs
    requests: ElasticRequestConfigs


class Configuration(BaseModel):
    core: CoreConfiguration
    data: DataConfiguration
    elasticsearch: ElasticConfiguration


# ---------------- cerberus configuration schema objects -------------------
core_options = {
    'debug':                {'type': 'boolean'},
    'verbosity':            {'type': 'integer'},
    'stats':                {'type': 'boolean'},
    'pipelines':            {'type': 'integer'},
    'shipper_threads':      {'type': 'integer'},
    'fetcher_threads':      {'type': 'integer'}
}
data_options = {
    'file_extension':           {'type': 'string'},
    'ignore_field_prefixes':    {'type': 'list',
                                 'nullable': True,
                                 'schema': {'type': 'string'}
                                 },
    'include':                  {'type': 'list',
                                 'nullable': True,
                                 'schema': {'type': 'string'}
                                 },
    'exclude':                  {'type': 'list',
                                 'nullable': True,
                                 'schema': {'type': 'string'}
                                 }
}
elastic_options = {
    'host':                     {'type': 'string'},
    'access':                   {'type': 'dict',
                                 'schema': {
                                     'user': {'type': 'string'},
                                     'password': {'type': 'string'},
                                     'ask_password': {'type': 'boolean'},
                                     'enable_ssl': {'type': 'boolean'},
                                     'ssl_cert': {'type': 'string'}
                                 }},
    'connection':               {'type': 'dict',
                                 'schema': {
                                     'max_retries': {'type': 'integer'},
                                     'retry_on_timeout': {'type': 'boolean'},
                                     'timeout': {'type': 'integer'}
                                 }},
    'sniffing':                 {'type': 'dict',
                                 'schema': {
                                     'disable_sniffing': {'type': 'boolean'},
                                     'sniff_on_start': {'type': 'boolean'},
                                     'sniff_on_connection_fail': {'type': 'boolean'},
                                     'sniff_timeout': {'type': 'integer'}
                                 }},
    'index':                    {'type': 'dict',
                                 'schema': {
                                     'index_template': {'type': 'string'},
                                     'index_prefix': {'type': 'string'},
                                     'rollover_size': {'type': 'integer'}
                                 }},
    'requests':                 {'type': 'dict',
                                 'schema': {
                                     'bulk_size': {'type': 'integer'},
                                     'bulk_fetch_size': {'type': 'integer'}
                                 }}
}
config_schema = {
    'core': {'type': 'dict',
             'schema': core_options},
    'data': {'type': 'dict',
             'schema': data_options},
    'elasticsearch': {'type': 'dict',
                      'schema': elastic_options}
    }

