import os
import time
import queue
import unicodecsv
from threading import Thread

from pydat.ingest.config import LOGGER


class FileReader(Thread):
    """Simple data file organizer

    This class focuses on iterating through directories and putting
    found files into a queue for processing by pipelines
    """

    def __init__(self, datafile_queue, eventTracker, options, **kwargs):
        Thread.__init__(self, **kwargs)
        self.datafile_queue = datafile_queue
        self.eventTracker = eventTracker
        self.options = options
        self._shutdown = False

    def shutdown(self):
        self._shutdown = True

    def run(self):
        try:
            if self.options.directory:
                self.scan_directory(self.options.directory)
            elif self.options.file:
                self.datafile_queue.put(self.options.file)
            else:
                LOGGER.error("File or Directory required")
        except Exception as e:
            LOGGER.error("Unknown exception in File Reader")
        finally:
            self.datafile_queue.join()
            LOGGER.debug("Setting FileReaderDone event")
            self.eventTracker.setFileReaderDone()

    def scan_directory(self, directory):
        for path in sorted(os.listdir(directory)):
            fp = os.path.join(directory, path)

            if os.path.isdir(fp):
                    self.scan_directory(fp)
            elif os.path.isfile(fp):
                if self._shutdown:
                    return
                if self.options.extension != '':
                    fn, ext = os.path.splitext(path)
                    if ext == '' or not ext.endswith(self.options.extension):
                        continue
                self.datafile_queue.put(fp)
            else:
                LOGGER.warning("%s is neither a file nor directory" % (fp))


class DataReader(Thread):
    """CSV Parsing Class

    This class focuses on reading in and parsing a given CSV file as
    provided by the FileReader class. After creating its output it
    places it on a queue for processing by the fetcher
    """

    def __init__(self, datafile_queue, data_queue, eventTracker,
                 options, **kwargs):
        Thread.__init__(self, **kwargs)
        self.datafile_queue = datafile_queue
        self.data_queue = data_queue
        self.options = options
        self.eventTracker = eventTracker
        self._shutdown = False
        self._pause = False

    def shutdown(self):
        self._shutdown = True

    def pause(self):
        self._pause = True

    def unpause(self):
        self._pause = False

    def run(self):
        while not self._shutdown:
            try:
                datafile = self.datafile_queue.get(True, 0.2)
                try:
                    self.parse_csv(datafile)
                finally:
                    self.datafile_queue.task_done()
            except queue.Empty as e:
                if self.eventTracker.fileReaderDone:
                    LOGGER.debug("FileReaderDone Event seen")
                    break
            except Exception as e:
                LOGGER.exception("Unhandled Exception")
        LOGGER.debug("Reader exiting")

    def check_header(self, header):
        for field in header:
            if field == "domainName":
                return True

        return False

    def parse_csv(self, filename):
        if self._shutdown:
            return

        try:
            csvfile = open(filename, 'rb')
            s = os.stat(filename)
            if s.st_size == 0:
                LOGGER.warning("File %s empty" % (filename))
                return
        except Exception as e:
            LOGGER.warning("Unable to stat file %s, skiping" % (filename))
            return

        if self.options.verbose:
            LOGGER.info("Processing file: %s" % filename)

        try:
            dnsreader = unicodecsv.reader(csvfile, strict=True,
                                          skipinitialspace=True)
        except Exception as e:
            LOGGER.exception("Unable to setup csv reader for file %s"
                             % (filename))
            return

        try:
            header = next(dnsreader)
        except Exception as e:
            LOGGER.exception("Unable to iterate through csv file %s"
                             % (filename))
            return

        try:
            if not self.check_header(header):
                raise unicodecsv.Error('CSV header not found')

            for row in dnsreader:
                while self._pause:
                    if self._shutdown:
                        LOGGER.debug("Shutdown received while paused")
                        break
                    time.sleep(.5)
                if self._shutdown:
                    LOGGER.debug("Shutdown received")
                    break
                if row is None or not row:
                    LOGGER.warning("Skipping empty row in file %s"
                                   % (filename))
                    continue
                self.data_queue.put({'header': header, 'row': row})
        except unicodecsv.Error as e:
            LOGGER.exception("CSV Parse Error in file %s - line %i\n"
                             % (os.path.basename(filename),
                                dnsreader.line_num))
        except Exception as e:
            LOGGER.exception("Unable to process file %s" % (filename))


class DataWorker(Thread):
    """Class to focus on entry comparison and instruction creation

    This class takes the input entry and latest entry as found by the fetcher
    and creates one or more elasticsearch update requests to be sent by the
    shipper
    """

    def __init__(self, work_queue, insert_queue, statTracker,
                 eventTracker, options, **kwargs):
        Thread.__init__(self, **kwargs)
        self.work_queue = work_queue
        self.insert_queue = insert_queue
        self.statTracker = statTracker
        self.options = options
        self.eventTracker = eventTracker
        self._shutdown = False
        self._finish = False

    def shutdown(self):
        self._shutdown = True

    def finish(self):
        self._finish = True

    def run(self):
        try:
            while not self._shutdown:
                try:
                    (entry, current_entry_raw) = self.work_queue.get_nowait()
                except queue.Empty as e:
                    if self._finish:
                        break
                    time.sleep(.0001)
                    continue
                except Exception as e:
                    LOGGER.exception("Unhandled Exception")

                try:
                    if entry is None:
                        LOGGER.warning("Malformed Entry")
                        continue

                    if (not self.options.redo or
                            self.update_required(current_entry_raw)):
                        self.statTracker.incr('total')
                        self.process_entry(entry, current_entry_raw)
                finally:
                    self.work_queue.task_done()

        except Exception as e:
            LOGGER.exception("Unhandled Exception")

    def update_required(self, current_entry):
        if current_entry is None:
            return True

        if current_entry['_source'][VERSION_KEY] == self.options.identifier:
            # This record already up to date
            return False
        else:
            return True

    def process_entry(self, entry, current_entry_raw):
        domainName = entry['domainName']
        details = entry['details']
        api_commands = []

        if current_entry_raw is not None:
            current_index = current_entry_raw['_index']
            current_id = current_entry_raw['_id']
            current_type = current_entry_raw['_type']
            current_entry = current_entry_raw['_source']

            if (not self.options.update and
                    (current_entry[VERSION_KEY] == self.options.identifier)):
                # Duplicate entry in source csv's?
                if self.options.vverbose:
                    LOGGER.info('%s: Duplicate' % domainName)
                self.statTracker.incr('duplicates')
                return

            if self.options.exclude is not None:
                details_copy = details.copy()
                for exclude in self.options.exclude:
                    del details_copy[exclude]

                changed = (set(details_copy.items()) -
                           set(current_entry['details'].items()))

            elif self.options.include is not None:
                details_copy = {}
                for include in self.options.include:
                    try:  # TODO
                        details_copy[include] = details[include]
                    except Exception as e:
                        pass

                changed = (set(details_copy.items()) -
                           set(current_entry['details'].items()))

            else:
                changed = set(details.items()) \
                            - set(current_entry['details'].items())

                # The above diff doesn't consider keys that are only in the
                # latest in es, so if a key is just removed, this diff will
                # indicate there is no difference even though a key had been
                # removed. I don't forsee keys just being wholesale removed,
                # so this shouldn't be a problem

            for ch in changed:
                self.statTracker.addChanged(ch[0])

            if len(changed) > 0:
                self.statTracker.incr('updated')
                if self.options.vverbose:
                    if self.options.update:
                        LOGGER.info("%s: Re-Registered/Transferred"
                                    % domainName)
                    else:
                        LOGGER.info("%s: Updated" % domainName)

                # Copy old entry into different document
                doc_id = "%s#%d.%d" % (current_id, current_entry[VERSION_KEY],
                                       current_entry.get(UPDATE_KEY, 0))
                if self.options.vverbose:
                    LOGGER.info("doc_id: %s" % (doc_id))
                api_commands.append(
                    self.process_command('create',
                                         self.options.indexNames.delta_write,
                                         doc_id,
                                         current_type,
                                         current_entry))

                # Update latest/orig entry
                if not self.options.update:
                    entry[FIRST_SEEN] = current_entry[FIRST_SEEN]
                api_commands.append(self.process_command('index',
                                                         current_index,
                                                         current_id,
                                                         current_type,
                                                         entry))
            else:
                self.statTracker.incr('unchanged')
                if self.options.vverbose:
                    LOGGER.info("%s: Unchanged" % domainName)
                doc_diff = {'doc': {VERSION_KEY: self.options.identifier,
                                    'details': details}}
                api_commands.append(self.process_command(
                                                     'update',
                                                     current_index,
                                                     current_id,
                                                     current_type,
                                                     doc_diff))
        else:
            self.statTracker.incr('new')
            if self.options.vverbose:
                LOGGER.info("%s: New" % domainName)
            (domain_name_only, tld) = parse_domain(domainName)
            doc_id = "%s.%s" % (tld, domain_name_only)
            if self.options.update:
                api_commands.append(
                    self.process_command('index',
                                         self.options.indexNames.orig_write,
                                         doc_id,
                                         DOC_TYPE,
                                         entry))
            else:
                api_commands.append(
                    self.process_command('create',
                                         self.options.indexNames.orig_write,
                                         doc_id,
                                         DOC_TYPE,
                                         entry))
        for command in api_commands:
            self.insert_queue.put(command)

    def process_command(self, request, index, _id, _type, entry=None):
        if request == 'create':
            command = {
                       "_op_type": "create",
                       "_index": index,
                       "_type": _type,
                       "_id": _id,
                       "_source": entry
                      }
            return command
        elif request == 'update':
            command = {
                       "_op_type": "update",
                       "_index": index,
                       "_id": _id,
                       "_type": _type,
                      }
            command.update(entry)
            return command
        elif request == 'delete':
            command = {
                        "_op_type": "delete",
                        "_index": index,
                        "_id": _id,
                        "_type": _type,
                      }
            return command
        elif request == 'index':
            command = {
                        "_op_type": "index",
                        "_index": index,
                        "_type": _type,
                        "_source": entry
                      }
            if _id is not None:
                command["_id"] = _id
            return command
        else:
            LOGGER.error("Unrecognized command")
            return None


def parse_domain(domainName):
    parts = domainName.rsplit('.', 1)
    try:
        return parts[0], parts[1]
    except IndexError as e:
        LOGGER.exception("Unable to parse domain '%s'" % (domainName))
        raise

