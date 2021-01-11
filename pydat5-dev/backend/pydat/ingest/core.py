import time
import queue
import multiprocessing
from threading import Thread
from multiprocessing import Process

from pydat.ingest.config import LOGGER
from pydat.ingest.util import StatTracker, CentralLogger, StatClient, LogClient


class Job:
    def __init__(self):
        pass


class PydatIngestor:
    def __init__(self):
        pass

    def initialize(self):
        pass

    def ingest(self):
        """
        setup and spawn all processes
        loop over the event tracker
        check if index needs rollover

        """
        # Setup logger
        logger = mpLogger(name="populater", debug=options.debug)
        logger.start()

        global LOGGER
        LOGGER = logger.getLogger()
        LOGGER.prefix = "(Main) "

        # Local Logger instance since myLogger relies on a queue
        # This can cause an issue if exiting since it doesn't give
        # it enough time to run through the queue
        myLogger = logger.logger

        # Resolve index names based on prefix information
        indexNames = indexFormatter(options.index_prefix)
        # Shove it into options
        options.indexNames = indexNames

        # Setup Data Queue
        datafile_queue = jmpQueue(maxsize=10000)

        # Grab elasticsearch python library version
        major = elasticsearch.VERSION[0]

        # Verify connectivity and version(s) of cluster
        try:
            es = elasticsearch.Elasticsearch(**options.es_args)
        except elasticsearch.exceptions.TransportError as e:
            myLogger.exception("Unable to connect to ElasticSearch")
            sys.exit(1)

        try:
            es_versions = []
            for version in es.cat.nodes(h='version').strip().split('\n'):
                es_versions.append([int(i) for i in version.split('.')])
        except Exception as e:
            myLogger.exception(("Unable to retrieve destination ElasticSearch "
                                "version"))
            sys.exit(1)

        es_major = 0
        for version in es_versions:
            if version[0] > es_major:
                es_major = version[0]
            if version[0] < 5 or (version[0] >= 5 and version[1] < 2):
                myLogger.error(("Destination ElasticSearch version must be "
                                "5.2 or greater"))
                sys.exit(1)

        if es_major != major:
            myLogger.error(("Python library installed does not "
                            "match with greatest (major) version in cluster"))
            sys.exit(1)

        # Setup template
        data_template = None
        base_path = os.path.dirname(os.path.realpath(__file__))
        template_path = os.path.join(base_path, "es_templates", "data.template")

        if not os.path.exists(template_path):
            myLogger.error("Unable to find template at %s" % (template_path))
            sys.exit(1)

        with open(template_path, 'r') as dtemplate:
            try:
                data_template = json.loads(dtemplate.read())
            except Exception as e:
                myLogger.exception("Unable to read template file")
                sys.exit(1)

        if options.config_template_only:
            configTemplate(es, major, data_template, options)
            sys.exit(0)

        metadata = None
        version_identifier = 0
        previousVersion = 0

        # Create the stats tracker thread
        statTracker = MainStatTracker()
        statTracker.daemon = True
        statTracker.start()

        # Create the metadata index if it doesn't exist
        if not es.indices.exists(indexNames.meta):
            if options.redo or options.update:
                myLogger.error(("Script cannot conduct a redo or update when no "
                                "initial data exists"))
                sys.exit(1)

            if options.identifier <= 0:
                myLogger.error("Identifier must be greater than 0")
                sys.exit(1)

            version_identifier = options.identifier

            # Setup the template
            configTemplate(es, major, data_template, options)

            # Create the metadata index with only 1 shard, even with
            # thousands of imports this index shouldn't warrant multiple shards
            # Also use the keyword analyzer since string analysis is not important
            meta_body = {"settings":
                             {"index":
                                  {"number_of_shards": 1,
                                   "analysis":
                                       {"analyzer":
                                            {"default":
                                                 {"type": "keyword"}}}}}}
            es.indices.create(index=indexNames.meta,
                              body=meta_body)

            # Create the 0th metadata entry
            metadata = {"metadata": 0,
                        "firstVersion": options.identifier,
                        "lastVersion": options.identifier}

            es.create(index=indexNames.meta,
                      doc_type=META_DOC_TYPE,
                      id=0,
                      body=metadata)

            # Create the first whois rollover index
            index_name = "%s-000001" % (options.index_prefix)
            es.indices.create(index=index_name,
                              body={"aliases": {indexNames.orig_write: {},
                                                indexNames.orig_search: {}}})

            # Create the first whois delta rollover index
            delta_name = "%s-delta-000001" % (options.index_prefix)
            es.indices.create(index=delta_name,
                              body={"aliases": {indexNames.delta_write: {},
                                                indexNames.delta_search: {}}})

            options.firstImport = True
        else:  # Data exists in the cluster
            try:
                result = es.get(index=indexNames.meta, doc_type=DOC_TYPE, id=0)
                if result['found']:
                    metadata = result['_source']
                else:
                    myLogger.error("Metadata index found but contains no data!!")
                    sys.exit(1)
            except Exception as e:
                myLogger.exception("Error fetching metadata from index")
                sys.exit(1)

            if options.identifier is not None:
                if options.identifier < 1:
                    myLogger.error("Identifier must be greater than 0")
                    sys.exit(1)
                if metadata['lastVersion'] >= options.identifier:
                    myLogger.error(("Identifier must be 'greater than' "
                                    "previous identifier"))
                    sys.exit(1)

                version_identifier = options.identifier
                previousVersion = metadata['lastVersion']
            else:  # redo or update
                result = es.search(index=indexNames.meta,
                                   body={"query": {"match_all": {}},
                                         "sort": [{"metadata":
                                                       {"order": "asc"}}],
                                         "size": 9999})

                if result['hits']['total'] == 0:
                    myLogger.error("Unable to fetch entries from metadata index\n")
                    sys.exit(1)

                lastEntry = result['hits']['hits'][-1]['_source']
                previousVersion = int(result['hits']['hits'][-2]['_id'])
                version_identifier = int(metadata['lastVersion'])
                if options.redo and (lastEntry.get('updateVersion', 0) > 0):
                    myLogger.error(("A Redo is only valid on recovering from a "
                                    "failed import via the -i flag.\nAfter "
                                    "ingesting a daily update, it is no longer "
                                    "available"))
                    sys.exit(1)

        options.previousVersion = previousVersion
        options.updateVersion = 0

        if options.exclude != "":
            options.exclude = options.exclude.split(',')
        else:
            options.exclude = None

        if options.include != "":
            options.include = options.include.split(',')
        else:
            options.include = None

        # Redo or Update Mode
        if options.redo or options.update:
            # Get the record for the attempted import
            version_identifier = int(metadata['lastVersion'])
            options.identifier = version_identifier
            try:
                previous_record = es.get(index=indexNames.meta,
                                         doc_type=DOC_TYPE,
                                         id=version_identifier)['_source']
            except Exception as e:
                myLogger.exception(("Unable to retrieve information "
                                    "for last import"))
                sys.exit(1)

            if 'excluded_keys' in previous_record:
                options.exclude = previous_record['excluded_keys']
            elif options.redo:
                options.exclude = None

            if 'included_keys' in previous_record:
                options.include = previous_record['included_keys']
            elif options.redo:
                options.include = None

            options.comment = previous_record['comment']

            statTracker.seed({'total': int(previous_record['total']),
                              'new': int(previous_record['new']),
                              'updated': int(previous_record['updated']),
                              'unchanged': int(previous_record['unchanged']),
                              'duplicates': int(previous_record['duplicates'])})

            if options.update:
                options.updateVersion = \
                    int(previous_record.get('updateVersion', 0)) + 1

            statTracker.seedChanged(previous_record['changed_stats'])

            if options.verbose:
                if options.redo:
                    myLogger.info(("Re-importing for: \n\tIdentifier: "
                                   "%s\n\tComment: %s")
                                  % (version_identifier, options.comment))
                else:
                    myLogger.info(("Updating for: \n\tIdentifier: %s\n\t"
                                   "Comment: %s")
                                  % (version_identifier, options.comment))

            # No need to update lastVersion or create metadata entry
        # Insert(normal) Mode
        else:
            # Update the lastVersion in the metadata
            es.update(index=indexNames.meta, id=0,
                      doc_type=META_DOC_TYPE,
                      body={'doc': {'lastVersion': options.identifier}})

            # Create the entry for this import
            meta_struct = {'metadata': options.identifier,
                           'updateVersion': 0,
                           'comment': options.comment,
                           'total': 0,
                           'new': 0,
                           'updated': 0,
                           'unchanged': 0,
                           'duplicates': 0,
                           'changed_stats': {}}

            if options.exclude is not None:
                meta_struct['excluded_keys'] = options.exclude
            elif options.include is not None:
                meta_struct['included_keys'] = options.include

            es.create(index=indexNames.meta, id=options.identifier,
                      doc_type=META_DOC_TYPE, body=meta_struct)

            # Resolve the alias to get the raw index names
        index_list = es.indices.get_alias(name=indexNames.orig_search)
        options.INDEX_LIST = sorted(index_list.keys(), reverse=True)

        pipelines = []
        # Check if rollover is required before starting any pipelines
        roll = rolloverRequired(es, options)
        if roll:
            rolloverIndex(roll, es, options, pipelines)

        # Everything configured -- start it up
        # Start up Reader Thread
        reader_thread = FileReader(datafile_queue, eventTracker, options)
        reader_thread.daemon = True
        reader_thread.start()

        for pipeline_id in range(options.procs):
            p = DataProcessor(pipeline_id, datafile_queue, None, None,
                              None, options)
            p.start()
            p.statTracker = statTracker
            p.eventTracker = eventTracker
            p.logger = logger
            pipelines.append(p)

        # Everything should be running at this point

        try:
            # Rollover check sequence
            def rolloverTimer(timer):
                now = time.time()
                # Check every 30 seconds
                if now - timer >= 30:
                    timer = now
                    roll = rolloverRequired(es, options)
                    if roll:
                        rolloverIndex(roll, es, options, pipelines)

                return timer

            timer = time.time()
            while True:
                timer = rolloverTimer(timer)

                # If bulkError occurs stop processing
                if eventTracker.bulkError:
                    myLogger.critical("Bulk API error -- forcing program shutdown")
                    raise KeyboardInterrupt(("Error response from ES worker, "
                                             "stopping processing"))

                # Wait for the pipelines to clear out the datafile queue
                # This means all files are currently being looked at or have
                # been processed
                if not reader_thread.is_alive() and datafile_queue.empty():
                    if options.verbose:
                        myLogger.info(("All files processed ... please wait for "
                                       "processing to complete ..."))
                    break

                time.sleep(.1)

            # Wait on pipelines to finish up
            for proc in pipelines:
                while proc.is_alive():
                    timer = rolloverTimer(timer)
                    proc.join(.1)

                    if eventTracker.bulkError:
                        myLogger.critical(("Bulk API error -- forcing program "
                                           "shutdown"))
                        raise KeyboardInterrupt(("Error response from ES worker, "
                                                 "stopping processing"))

            try:
                # Since this is the shutdown section, ignore Keyboard Interrupts
                # especially since the interrupt code (below) does effectively
                # the same thing

                statTracker.shutdown()
                statTracker.join()

                # Update the stats
                try:
                    es.update(index=indexNames.meta,
                              id=version_identifier,
                              doc_type=META_DOC_TYPE,
                              body={'doc': {'updateVersion': options.updateVersion,
                                            'total': statTracker.total,
                                            'new': statTracker.new,
                                            'updated': statTracker.updated,
                                            'unchanged': statTracker.unchanged,
                                            'duplicates': statTracker.duplicates,
                                            'changed_stats':
                                                statTracker.changed_stats}})
                except Exception as e:
                    myLogger.exception("Error attempting to update stats")
            except KeyboardInterrupt:
                myLogger.info("Please wait for processing to complete ...")

            try:
                datafile_queue.close()
            except Exception as e:
                myLogger.debug("Exception closing queues", exc_info=True)

            if options.verbose:
                myLogger.info("Done ...")

            if options.stats:
                myLogger.info("\nStats:\n"
                              + "Total Entries:\t\t %d\n" % statTracker.total
                              + "New Entries:\t\t %d\n" % statTracker.new
                              + "Updated Entries:\t %d\n" % statTracker.updated
                              + "Duplicate Entries\t %d\n" % statTracker.duplicates
                              + "Unchanged Entries:\t %d\n"
                              % statTracker.unchanged)

            # Ensure logger processes all messages
            logger.join()
        except KeyboardInterrupt as e:
            myLogger.warning(("Cleaning Up ... Please Wait ...\nWarning!! "
                              "Forcefully killing this might leave Elasticsearch "
                              "in an inconsistent state!"))
            # Tell the reader to halt activites
            reader_thread.shutdown()

            # Flush the queue if the reader is alive so it can see the
            # shutdown event in case it's blocked on a put
            myLogger.info("Shutting down input reader threads ...")
            while reader_thread.is_alive():
                try:
                    datafile_queue.get_nowait()
                    datafile_queue.task_done()
                except queue.Empty:
                    break

            myLogger.debug("Draining data queue")
            # Drain the data file queue
            while not datafile_queue.empty():
                try:
                    datafile_queue.get_nowait()
                    datafile_queue.task_done()
                except queue.Empty:
                    break

            reader_thread.join()

            # Signal the pipeline procs to shutdown
            eventTracker.setShutdown()

            myLogger.info("Shutting down processing pipelines...")
            for proc in pipelines:
                while proc.is_alive():
                    proc.join(.1)

            myLogger.debug("Pipelines shutdown, cleaning up state")
            # Send the finished message to the stats queue to shut it down
            statTracker.shutdown()
            statTracker.join()

            # Attempt to update the stats
            try:
                myLogger.info("Finalizing metadata")
                es.update(index=indexNames.meta,
                          id=options.identifier,
                          doc_type=META_DOC_TYPE,
                          body={'doc': {'total': statTracker.total,
                                        'new': statTracker.new,
                                        'updated': statTracker.updated,
                                        'unchanged': statTracker.unchanged,
                                        'duplicates': statTracker.duplicates,
                                        'changed_stats':
                                            statTracker.changed_stats}})
            except Exception as e:
                myLogger.warning(("Unable to finalize stats, data may be out of "
                                  "sync"), exc_info=True)

            myLogger.info("Finalizing settings")
            try:
                datafile_queue.close()
            except Exception as e:
                myLogger.debug("Exception closing queues", exc_info=True)

            myLogger.info("... Done")

            # Ensure logger processes all messages
            logger.join()
            sys.exit(0)

    def startprocess(self):
        pass


class EventTracker(object):
    def __init__(self):
        self._shutdownEvent = multiprocessing.Event()
        self._bulkErrorEvent = multiprocessing.Event()
        self._fileReaderDoneEvent = multiprocessing.Event()

    @property
    def shutdown(self):
        return self._shutdownEvent.is_set()

    def setShutdown(self):
        self._shutdownEvent.set()

    @property
    def bulkError(self):
        return self._bulkErrorEvent.is_set()

    def setBulkError(self):
        self._bulkErrorEvent.set()

    @property
    def fileReaderDone(self):
        return self._fileReaderDoneEvent.is_set()

    def setFileReaderDone(self):
        self._fileReaderDoneEvent.set()


class DataProcessor(Process):
    """Main pipeline process which manages individual threads
    """
    def __init__(self, pipeline_id, datafile_queue, statTracker, logger,
                 eventTracker, options, **kwargs):
        Process.__init__(self, **kwargs)
        self.datafile_queue = datafile_queue
        self.statTracker = statTracker
        self.options = options
        self.fetcher_threads = []
        self.worker_thread = None
        self.shipper_threads = []
        self.reader_thread = None
        self.pause_request = multiprocessing.Value('b', False)
        self._paused = multiprocessing.Value('b', False)
        self._complete = multiprocessing.Value('b', False)
        self.eventTracker = eventTracker
        self.es = None
        self.myid = pipeline_id
        self.logger = logger

        # These are created when the process starts up
        self.data_queue = None
        self.work_queue = None
        self.insert_queue = None

    @property
    def paused(self):
        return self._paused.value

    @property
    def complete(self):
        return self._complete.value

    def pause(self):
        self.pause_request.value = True

    def unpause(self):
        self.pause_request.value = False

    def update_index_list(self):
        index_list = \
            self.es.indices.get_alias(name=self.options.indexNames.orig_search)
        self.options.INDEX_LIST = sorted(index_list.keys(), reverse=True)
        self.options.rolledOver = True

    def _pause(self):
        if self.reader_thread.isAlive():
            try:
                self.reader_thread.pause()
                self.finish()
                self._paused.value = True
            except Exception as e:
                LOGGER.exception("Unable to pause reader thread")
        else:
            LOGGER.debug("Pause requested when reader thread not alive")

    def _unpause(self):
        if self.reader_thread.isAlive():
            try:
                self.reader_thread.unpause()
            except Exception as e:
                LOGGER.exception("Unable to unpause reader thread")
            self.update_index_list()
            self.startup_rest()
            self._paused.value = False
        else:
            LOGGER.debug("Pause requested when reader thread not alive")

    def shutdown(self):
        LOGGER.debug("Shutting down reader")
        self.reader_thread.shutdown()
        while self.reader_thread.is_alive():
            try:
                self.datafile_queue.get_nowait()
                self.datafile_queue.task_done()
            except queue.Empty:
                break

        LOGGER.debug("Shutting down fetchers")
        for fetcher in self.fetcher_threads:
            fetcher.shutdown()
            # Ensure put is not blocking shutdown
            while fetcher.is_alive():
                try:
                    self.work_queue.get_nowait()
                    self.work_queue.task_done()
                except queue.Empty:
                    break

        LOGGER.debug("Draining work queue")
        # Drain the work queue
        while not self.work_queue.empty():
            try:
                self.work_queue.get_nowait()
                self.work_queue.task_done()
            except queue.Empty:
                break

        LOGGER.debug("Shutting down worker thread")
        self.worker_thread.shutdown()
        while self.worker_thread.is_alive():
            try:
                self.insert_queue.get_nowait()
                self.insert_queue.task_done()
            except queue.Empty:
                break

        LOGGER.debug("Draining insert queue")
        # Drain the insert queue
        while not self.insert_queue.empty():
            try:
                self.insert_queue.get_nowait()
                self.insert_queue.task_done()
            except queue.Empty:
                break

        LOGGER.debug("Waiting for shippers to finish")
        # Shippers can't be forced to shutdown
        for shipper in self.shipper_threads:
            shipper.finish()
            shipper.join()

        LOGGER.debug("Shutdown Complete")

    def finish(self):
        LOGGER.debug("Waiting for fetchers to finish")
        for fetcher in self.fetcher_threads:
            fetcher.finish()
            fetcher.join()

        LOGGER.debug("Waiting for worker to finish")
        self.worker_thread.finish()
        self.worker_thread.join()

        LOGGER.debug("Waiting for shippers to finish")
        for shipper in self.shipper_threads:
            shipper.finish()
            shipper.join()

        LOGGER.debug("Finish Complete")

    def startup_rest(self):
        LOGGER.debug("Starting Worker")
        self.worker_thread = DataWorker(self.work_queue,
                                        self.insert_queue,
                                        self.statTracker,
                                        self.eventTracker,
                                        self.options)
        self.worker_thread.daemon = True
        self.worker_thread.start()

        LOGGER.debug("starting Fetchers")
        for _ in range(self.options.fetcher_threads):
            fetcher_thread = DataFetcher(self.es,
                                         self.data_queue,
                                         self.work_queue,
                                         self.eventTracker,
                                         self.options)
            fetcher_thread.start()
            self.fetcher_threads.append(fetcher_thread)

        LOGGER.debug("Starting Shippers")
        for _ in range(self.options.shipper_threads):
            shipper_thread = DataShipper(self.es,
                                         self.insert_queue,
                                         self.eventTracker,
                                         self.options)
            shipper_thread.start()
            self.shipper_threads.append(shipper_thread)

    def run(self):
        os.setpgrp()
        global LOGGER
        LOGGER = self.logger.getLogger()
        LOGGER.prefix = "(Pipeline %d) " % (self.myid)

        # Queue for individual csv entries
        self.data_queue = queue.Queue(maxsize=10000)
        # Queue for current/new entry comparison
        self.work_queue = queue.Queue(maxsize=10000)
        # Queue for shippers to send data
        self.insert_queue = queue.Queue(maxsize=10000)

        try:
            self.es = elasticsearch.Elasticsearch(**self.options.es_args)
        except elasticsearch.exceptions.TransportError as e:
            LOGGER.critical("Unable to establish elastic connection")
            return

        self.startup_rest()

        LOGGER.debug("Starting Reader")
        self.reader_thread = DataReader(self.datafile_queue,
                                        self.data_queue,
                                        self.eventTracker,
                                        self.options)
        self.reader_thread.start()

        # Wait to shutdown/finish
        while 1:
            if self.eventTracker.shutdown:
                LOGGER.debug("Shutdown event received")
                self.shutdown()
                break

            if self.pause_request.value:
                self._pause()
                while self.pause_request.value:
                    time.sleep(.1)
                self._unpause()

            self.reader_thread.join(.1)
            if not self.reader_thread.isAlive():
                LOGGER.debug("Reader thread exited, finishing up")
                self.finish()
                break

            time.sleep(.1)
        LOGGER.debug("Pipeline Shutdown")
        self._complete.value = True

