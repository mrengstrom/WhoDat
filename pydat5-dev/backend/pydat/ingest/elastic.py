import time
import queue
import elasticsearch
from threading import Thread
from elasticsearch import helpers

from pydat.ingest.config import LOGGER


VERSION_KEY = 'dataVersion'
UPDATE_KEY = 'updateVersion'
FIRST_SEEN = 'dataFirstSeen'

DOC_TYPE = "doc"
META_DOC_TYPE = "doc"


class IndexFormatter(object):
    """Convenience object to store formatted index names, based on the prefix
    """

    def __init__(self, prefix):
        self.prefix = prefix
        self.orig_write = "%s-write" % prefix
        self.delta_write = "%s-delta-write" % prefix
        self.orig_search = "%s-orig" % prefix
        self.delta_search = "%s-delta" % prefix
        self.search = "%s-search" % prefix
        self.meta = ".%s-meta" % prefix
        self.template_pattern = "%s-*" % prefix
        self.template_name = "%s-template" % prefix


def config_template(es, major, data_template, options):
    if data_template is not None:
        # Process version specific template info
        if major == 5:
            # ES5 Templates use the "template" field
            data_template["template"] = options.indexNames.template_pattern
            # Disable "_all" field since it is handled customly, instead
            data_template["mappings"]["doc"]["_all"] = {
                "enabled": False
            }
        else:
            data_template["index_patterns"] = \
                [options.indexNames.template_pattern]

        # Shared template info
        data_template["aliases"][options.indexNames.search] = {}

        # Actually configure template
        es.indices.put_template(name=options.indexNames.template_name,
                                body=data_template)


def rollover_required(es, options):
    LOGGER.debug("Checking if rollover required")
    try:
        doc_count = int(es.cat.count(index=options.indexNames.orig_write,
                                     h="count"))
    except elasticsearch.exceptions.NotFoundError as e:
        LOGGER.warning("Unable to find required index\n")
    except Exception as e:
        LOGGER.exception("Unexpected exception\n")

    if doc_count > options.rollover_docs:
        return 1

    try:
        doc_count = int(es.cat.count(index=options.indexNames.delta_write,
                                     h="count"))
    except elasticsearch.exceptions.NotFoundError as e:
        LOGGER.warning("Unable to find required index\n")
    except Exception as e:
        LOGGER.exception("Unexpected exception\n")

    if doc_count > options.rollover_docs:
        return 2

    return 0


def rollover_index(roll, es, options, pipelines):

    if options.verbose:
        LOGGER.info("Rolling over ElasticSearch Index")

    # Pause the pipelines
    for proc in pipelines:
        proc.pause()

    LOGGER.debug("Waiting for pipelines to pause")
    # Ensure procs are paused
    for proc in pipelines:
        while not proc.paused:
            if proc.complete:
                break
            time.sleep(.1)
    LOGGER.debug("Pipelines paused")

    try:
        # Processing should have finished
        # Rollover the large index
        if roll == 1:
            write_alias = options.indexNames.orig_write
            search_alias = options.indexNames.orig_search
        elif roll == 2:
            write_alias = options.indexNames.delta_write
            search_alias = options.indexNames.delta_search

        try:
            orig_name = es.indices.get_alias(name=write_alias).keys()[0]
        except Exception as e:
            LOGGER.error("Unable to get/resolve index alias")

        try:
            result = es.indices.rollover(alias=write_alias,
                                         body={"aliases": {
                                                    search_alias: {}}})
        except Exception as e:
            LOGGER.exception("Unable to issue rollover command: %s")

        try:
            es.indices.refresh(index=orig_name)
        except Exception as e:
            LOGGER.exception("Unable to refresh rolled over index")

        # Index rolled over, restart processing
        for proc in pipelines:
            if not proc.complete:  # Only if process has data to process
                proc.unpause()

        if options.verbose:
            LOGGER.info("Roll over complete")

    except KeyboardInterrupt:
        LOGGER.warning(("Keyboard Interrupt ignored while rolling over "
                        "index, please wait a few seconds and try again"))


class DataFetcher(Thread):
    """Bulk Fetching of Records

    This class does a bulk fetch of records to make the comparison of
    records more efficient. It takes the response and bundles it up with
    the source to be sent to the worker
    """

    def __init__(self, es, data_queue, work_queue, eventTracker,
                 options, **kwargs):
        Thread.__init__(self, **kwargs)
        self.data_queue = data_queue
        self.work_queue = work_queue
        self.options = options
        self.es = es
        self.fetcher_threads = []
        self.eventTracker = eventTracker
        self._shutdown = False
        self._finish = False

    def shutdown(self):
        self._shutdown = True

    def finish(self):
        self._finish = True

    def run(self):
        try:
            fetch = list()
            while not self._shutdown:
                try:
                    work = self.data_queue.get_nowait()
                except queue.Empty as e:
                    if self._finish:
                        if len(fetch) > 0:
                            data = self.handle_fetch(fetch)
                            for item in data:
                                self.work_queue.put(item)
                        break
                    time.sleep(.01)
                    continue
                except Exception as e:
                    LOGGER.exception("Unhandled Exception")
                    continue

                try:
                    entry = self.parse_entry(work['row'], work['header'])
                    if entry is None:
                        LOGGER.warning("Malformed Entry")
                        continue

                    # Pre-empt all of this processing when not necessary
                    if (self.options.firstImport and
                            not self.options.rolledOver):
                        self.work_queue.put((entry, None))
                        continue

                    (domainName, tld) = parse_domain(entry['domainName'])
                    doc_id = "%s.%s" % (tld, domainName)
                    fetch.append((doc_id, entry))

                    if len(fetch) >= self.options.bulk_fetch_size:
                        start = time.time()
                        data = self.handle_fetch(fetch)
                        for item in data:
                            self.work_queue.put(item)
                        fetch = list()
                except Exception as e:
                    LOGGER.exception("Unhandled Exception")
                finally:
                    self.data_queue.task_done()

        except Exception as e:
            LOGGER.exception("Unhandled Exception")

    def parse_entry(self, input_entry, header):
        if len(input_entry) == 0:
            return None

        htmlparser = HTMLParser()

        details = {}
        domainName = ''
        for (i, item) in enumerate(input_entry):
            if any(header[i].startswith(s)
                    for s in self.options.ignore_field_prefixes):
                continue
            if header[i] == 'domainName':
                if self.options.vverbose:
                    LOGGER.info("Processing domain: %s" % item)
                domainName = item
                continue
            if item == "":
                details[header[i]] = None
            else:
                details[header[i]] = htmlparser.unescape(item)

        entry = {VERSION_KEY: self.options.identifier,
                 FIRST_SEEN: self.options.identifier,
                 'tld': parse_domain(domainName)[1],
                 'details': details,
                 'domainName': domainName}

        if self.options.update:
            entry[UPDATE_KEY] = self.options.updateVersion

        return entry

    def handle_fetch(self, fetch_list):
        results = list()
        try:
            docs = list()
            for (doc_id, entry) in fetch_list:
                for index_name in self.options.INDEX_LIST:
                    getdoc = {'_index': index_name,
                              '_type': DOC_TYPE,
                              '_id': doc_id}
                    docs.append(getdoc)
        except Exception as e:
            LOGGER.exception("Unable to generate doc list")
            return results

        try:
            result = self.es.mget(body={"docs": docs})
        except Exception as e:
            LOGGER.exception("Unable to create mget request")
            return results

        try:
            for (doc_count, index) in \
                    enumerate(range(0, len(result['docs']),
                              len(self.options.INDEX_LIST))):
                found = None
                doc_results = \
                    result['docs'][index:index + len(self.options.INDEX_LIST)]

                for res in doc_results:
                    if res['found']:
                        found = res
                        break

                if found is not None:
                    results.append((fetch_list[doc_count][1], found))
                else:
                    results.append((fetch_list[doc_count][1], None))

            return results
        except Exception as e:
            LOGGER.exception("Unhandled Exception")


class DataShipper(Thread):
    """Thread that ships commands to elasticsearch cluster_stats
    """

    def __init__(self, es, insert_queue, eventTracker,
                 options, **kwargs):
        Thread.__init__(self, **kwargs)
        self.insert_queue = insert_queue
        self.options = options
        self.eventTracker = eventTracker
        self.es = es
        self._finish = False

    def finish(self):
        self._finish = True

    def run(self):
        def bulkIter():
            while not (self._finish and self.insert_queue.empty()):
                try:
                    req = self.insert_queue.get_nowait()
                except queue.Empty:
                    time.sleep(.1)
                    continue

                try:
                    yield req
                finally:
                    self.insert_queue.task_done()

        try:
            for (ok, response) in \
                    helpers.streaming_bulk(self.es, bulkIter(),
                                           raise_on_error=False,
                                           chunk_size=self.options.bulk_size):
                resp = response[response.keys()[0]]
                if not ok and resp['status'] not in [404, 409]:
                        if not self.eventTracker.bulkError:
                            self.eventTracker.setBulkError()
                        LOGGER.debug("Response: %s" % (str(resp)))
                        LOGGER.error(("Error making bulk request, received "
                                      "error reason: %s")
                                     % (resp['error']['reason']))
        except Exception as e:
            LOGGER.exception("Unexpected error processing bulk commands")
            if not self.eventTracker.bulkError:
                self.eventTracker.setBulkError

