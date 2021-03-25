import sys
import yaml
import json
import getpass
import argparse
from pathlib import Path

from ingest.core import PydatIngestor
from ingest.config import Configuration, config_schema
from ingest.util import merge_nested_dictionaries


def main(options):
    with Path(options.get('config')).open('r') as c:
        config_file = yaml.safe_load(c)
    config_file = merge_nested_dictionaries(config_file, options)

    try:
        config = Configuration(**config_file)
    except Exception as e:
        print(e)
        sys.exit(1)

    if config.elasticsearch.access.ask_password:
        try:
            config.elasticsearch.access.password = getpass.getpass("Enter ElasticSearch Password: ")
        except Exception as e:
            print("Unable to get password")
            sys.exit(1)

    ingestor = PydatIngestor(config)
    ingestor.ingest()


def unflatten_argparse(args):
    def set_key(dictionary, keys, value):
        for key in keys[:-1]:
            dictionary = dictionary.setdefault(key, {})
        dictionary[keys[-1]] = value

    nested = dict()
    [set_key(nested, arg.split('__'), val) for arg, val in args.items() if val]
    return nested


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    data_source = parser.add_mutually_exclusive_group()
    data_source.add_argument("-f", "--file", action="store", dest="data__file",
                            default=None, help="Input CSV file")
    data_source.add_argument("-d", "--directory", action="store",
                            dest="data__directory", default=None,
                            help=("Directory to recursively search for CSV "
                                  "files -- mutually exclusive to '-f' "
                                  "option"))

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-i", "--identifier", action="store", dest="core__identifier",
                      type=int, default=None,
                      help=("Numerical identifier to use in update to "
                            "signify version (e.g., '8' or '20140120')"))
    mode.add_argument("-r", "--redo", action="store_true", dest="core__redo",
                      default=False,
                      help=("Attempt to re-import a failed import or import "
                            "more data, uses stored metadata from previous "
                            "import (-o, -n, and -x not required and will "
                            "be ignored!!)"))
    mode.add_argument("-z", "--update", action="store_true", dest="core__update",
                      default=False,
                      help=("Run the script in update mode. Intended for "
                            "taking daily whois data and adding new domains "
                            "to the current existing index in ES."))
    mode.add_argument("--config-template-only", action="store_true",
                      default=False, dest="core__config_template_only",
                      help=("Configure the ElasticSearch template and "
                            "then exit"))

    parser.add_argument("-v", "--verbose", action="count", dest="core__verbosity",
                        default=None, help="Be verbose, add extra v's for more verbosity (will override config file)")
    parser.add_argument("--debug", action="store_true", default=False, dest="core__debug",
                        help="Enables debug logging (will override config file)")

    parser.add_argument("-o", "--comment", action="store", dest="comment",
                        default="", help="Comment to store with metadata")

    parser.add_argument("--config", action="store", dest="config",
                        type=str, default="pydat/scripts/ingest/configuration/es_populate_config.yaml",
                        help=("path to a config file specifying the elasticsearch"
                              "instance settings and parameters"))

    main(unflatten_argparse(vars(parser.parse_args())))
