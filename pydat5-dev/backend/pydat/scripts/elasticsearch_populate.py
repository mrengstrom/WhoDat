import sys
import yaml
import getpass
import argparse
import multiprocessing

from pydat.ingest.core import PydatIngestor


def main(options):
    es_args = {
        'hosts': options.es_uri,
        'sniff_on_start': (not options.es_disable_sniffing),
        'sniff_on_connection_fail': (not options.es_disable_sniffing),
        'sniff_timeout': (None if options.es_disable_sniffing else 1000),
        'max_retries': 100,
        'retry_on_timeout': True,
        'timeout': 100
    }

    if options.es_ask_pass:
        try:
            options.es_pass = getpass.getpass("Enter ElasticSearch Password: ")
        except Exception as e:
            print("Unable to get password")
            sys.exit(1)

    if options.es_user is not None and options.es_pass is None:
        print("Password must be supplied along with a username")
        sys.exit(1)

    if options.es_user is not None and options.es_pass is not None:
        options.es_args['http_auth'] = (options.es_user,
                                        options.es_pass)

    if options.es_cacert is not None:
        options.es_args['use_ssl'] = True
        options.es_args['ca_certs'] = options.es_cacert

    if options.vverbose:
        options.verbose = True

    options.firstImport = False
    options.rolledOver = False

    # As these are crafted as optional args, but are really a required
    # mutually exclusive group, must check that one is specified
    if (not options.config_template_only and
            (options.file is None and options.directory is None)):
        print("A File or Directory source is required")
        parser.parse_args(["-h"])

    ingestor = PydatIngestor()
    ingestor.initialize()
    ingestor.ingest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    data_source = parser.add_mutually_exclusive_group()
    data_source.add_argument("-f", "--file", action="store", dest="file",
                            default=None, help="Input CSV file")
    data_source.add_argument("-d", "--directory", action="store",
                            dest="directory", default=None,
                            help=("Directory to recursively search for CSV "
                                  "files -- mutually exclusive to '-f' "
                                  "option"))

    parser.add_argument("-e", "--extension", action="store", dest="extension",
                        default='csv',
                        help=("When scanning for CSV files only parse "
                              "files with given extension (default: 'csv')"))

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-i", "--identifier", action="store", dest="identifier",
                      type=int, default=None,
                      help=("Numerical identifier to use in update to "
                            "signify version (e.g., '8' or '20140120')"))
    mode.add_argument("-r", "--redo", action="store_true", dest="redo",
                      default=False,
                      help=("Attempt to re-import a failed import or import "
                            "more data, uses stored metadata from previous "
                            "import (-o, -n, and -x not required and will "
                            "be ignored!!)"))
    mode.add_argument("-z", "--update", action="store_true", dest="update",
                      default=False,
                      help=("Run the script in update mode. Intended for "
                            "taking daily whois data and adding new domains "
                            "to the current existing index in ES."))
    mode.add_argument("--config-template-only", action="store_true",
                      default=False, dest="config_template_only",
                      help=("Configure the ElasticSearch template and "
                            "then exit"))

    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose",
                        default=False, help="Be verbose")
    parser.add_argument("--vverbose", action="store_true", dest="vverbose",
                        default=False,
                        help=("Be very verbose (Prints status of every "
                              "domain parsed, very noisy)"))
    parser.add_argument("-s", "--stats", action="store_true", dest="stats",
                        default=False, help="Print out Stats after running")
    parser.add_argument("--debug", action="store_true", default=False,
                        help="Enables debug logging")

    update_method = parser.add_mutually_exclusive_group()
    update_method.add_argument("-x", "--exclude", action="store",
                              dest="exclude", default="",
                              help=("Comma separated list of keys to exclude "
                                    "if updating entry"))
    update_method.add_argument("-n", "--include", action="store",
                              dest="include", default="",
                              help=("Comma separated list of keys to include "
                                    "if updating entry (mutually exclusive to "
                                    "-x)"))
    parser.add_argument("--ignore-field-prefixes", nargs='*',
                        dest="ignore_field_prefixes", type=str,
                        default=['zoneContact',
                                 'billingContact',
                                 'technicalContact'],
                        help=("list of fields (in whois data) to ignore when "
                              "extracting and inserting into ElasticSearch"))

    parser.add_argument("-o", "--comment", action="store", dest="comment",
                        default="", help="Comment to store with metadata")

    parser.add_argument("--pipelines", action="store", dest="procs", type=int,
                        metavar="PIPELINES",
                        default=2, help="Number of pipelines, defaults to 2")
    parser.add_argument("--shipper-threads", action="store",
                        dest="shipper_threads", type=int, default=1,
                        help=("How many threads per pipeline to spawn to send "
                              "bulk ES messages. The larger your cluster, "
                              "the more you can increase this, defaults to 1"))
    parser.add_argument("--fetcher-threads", action="store",
                        dest="fetcher_threads", type=int, default=2,
                        help=("How many threads to spawn to search ES. The "
                              "larger your cluster, the more you can "
                              "increase this, defaults to 2"))


    # es args, move to a config file
    parser.add_argument("-u", "--es-uri", nargs="*", dest="es_uri",
                        default=['localhost:9200'],
                        help=("Location(s) of ElasticSearch Server (e.g., "
                              "foo.server.com:9200) Can take multiple "
                              "endpoints"))
    parser.add_argument("--es-user", action="store", dest="es_user",
                        default=None,
                        help=("Username for ElasticSearch when Basic Auth"
                              "is enabled"))
    parser.add_argument("--es-pass", action="store", dest="es_pass",
                        default=None,
                        help=("Password for ElasticSearch when Basic Auth"
                              "is enabled"))
    parser.add_argument("--es-ask-pass", action="store_true",
                        dest="es_ask_pass", default=False,
                        help=("Prompt for ElasticSearch password"))
    parser.add_argument("--es-enable-ssl", action="store",
                        dest="es_cacert", default=None,
                        help=("The path, on disk to the cacert of the "
                              "ElasticSearch server to enable ssl/https "
                              "support"))
    parser.add_argument("--es-disable-sniffing", action="store_true",
                        dest="es_disable_sniffing", default=False,
                        help=("Disable ES sniffing, useful when ssl hostname"
                              "verification is not working properly"))
    parser.add_argument("-p", "--index-prefix", action="store",
                        dest="index_prefix", default='pydat',
                        help=("Index prefix to use in ElasticSearch "
                              "(default: pydat)"))
    parser.add_argument("-B", "--bulk-size", action="store", dest="bulk_size",
                        type=int, default=1000,
                        help="Size of Bulk Elasticsearch Requests")
    parser.add_argument("-b", "--bulk-fetch-size", action="store",
                        dest="bulk_fetch_size", type=int, default=50,
                        help=("Number of documents to search for at a time "
                              "(default 50), note that this will be "
                              "multiplied by the number of indices you "
                              "have, e.g., if you have 10 pydat-<number> "
                              "indices it results in a request for 500 "
                              "documents"))
    parser.add_argument("--rollover-size", action="store", type=int,
                        dest="rollover_docs", default=50000000,
                        help=("Set the number of documents after which point "
                              "a new index should be created, defaults to "
                              "50 milllion, note that this is fuzzy since "
                              "the index count isn't continuously updated, "
                              "so should be reasonably below 2 billion per "
                              "ES shard and should take your ES "
                              "configuration into consideration"))

    options = parser.parse_args()

    main(options)
