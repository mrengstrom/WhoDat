from typing import List

from pydantic import BaseModel, conint, root_validator


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
    ask_password: bool = False
    enable_ssl: bool = False
    ssl_cert: str = None

    @root_validator
    def validate_user_pass_pairs(self, values):
        if values.get('user') or values.get('password') or values.get('ask_password'):
            if not (values.get('user') and (values.get('password') or values.get('ask_password'))):
                raise ValueError('Both a user and password are required (or ask_password enabled)')
        return values

    @root_validator
    def validate_cert_if_enabled(self, values):
        if values.get('enable_ssl') and not values.get('ssl_cert'):
            raise ValueError('Please provide a ssl cert location when enabling ssl')
        return values


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
