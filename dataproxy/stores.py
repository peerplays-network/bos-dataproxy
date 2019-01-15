import io
import os
import uuid
import time
import threading
import shutil
import logging
from datetime import datetime
from . import datestring


def zip_it(folder):
    if os.path.isfile(folder + ".tar.gz"):
        logging.getLogger("RawStore").info("The target zip file " + folder + ".tar.gz" + " already exists on the server, renaming it ... ")
        os.rename(folder + ".tar.gz", folder + ".tar.gz.renamed." + time.strftime("%Y%M%d%S%s"))

    logging.getLogger("RawStore").info("Start zipping old folder " + folder)
    shutil.make_archive(folder, 'gztar', folder)
    logging.getLogger("RawStore").info("Zipping done, deleting old folder " + folder)
    shutil.rmtree(folder)
    logging.getLogger("RawStore").info("Deleting done, old folder was " + folder)


class RawStore(object):
    """ Stores the stream content as is in the file system """
    _CHUNK_SIZE_BYTES = 4096

    def __init__(self,
                 storage_path="dump/a_raw/{yearmonthdate}",
                 uuidgen=uuid.uuid4,
                 fopen=io.open,
                 dmakedir=os.makedirs):
        self._storage_path = storage_path
        self._uuidgen = uuidgen
        self._fopen = fopen
        self._dmakedir = dmakedir

    def get_storage_path(self, sub_folder, folder_time=None):
        if not folder_time:
            folder_time = time.time()
        date_folder = self._storage_path.format(
            yearmonthdate=time.strftime("%Y%m%d", time.localtime(folder_time))
        )
        folder = os.path.join(
            date_folder,
            sub_folder
        )

        # check if folder exists. if it doesnt, create it and zip the old one
        if not os.path.isdir(date_folder):
            # go back 23h in time
            old_date_folder = self._storage_path.format(
                yearmonthdate=time.strftime("%Y%m%d", time.localtime(folder_time - 60 * 60 * 23))
            )
            if os.path.isdir(old_date_folder):
                thr = threading.Thread(target=zip_it, args=(old_date_folder,), kwargs={})
                thr.start()  # we dont care when it finishes

        # ensure all subfolders exist
        self._dmakedir(folder, exist_ok=True)

        return folder

    def save(self, sub_folder, file_content):
        name = '{timestamp}_{uuid}{ext}'.format(
            timestamp=time.strftime("%Y%m%d-%H%M%S"),
            uuid=self._uuidgen(),
            ext='.raw')
        file_path = self.get_storage_path(sub_folder)
        file_name = os.path.join(
            file_path,
            name
        )

        with self._fopen(file_name, 'w') as file:
            file.write(file_content)

        return name, file_path


class FileStore(object):
    """ Stores parsed files in the file system """
    _CHUNK_SIZE_BYTES = 4096

    def __init__(self,
                 storage_path="dump/{yearmonthdate}/received",
                 uuidgen=uuid.uuid4,
                 fopen=io.open,
                 dmakedir=os.makedirs,
                 disfile=os.path.isfile,
                 disfolder=os.path.isdir):
        self._storage_path = storage_path
        self._uuidgen = uuidgen
        self._fopen = fopen
        self._dmakedir = dmakedir
        self._disfile = disfile
        self._disfolder = disfolder

        self._zip_old = False

    def _get_storage_path(self, sub_folder, folder_time=None):
        if not folder_time:
            folder_time = time.time()
        if type(folder_time) == datetime:
            folder_time = folder_time.timestamp()

        date_folder = self._storage_path.format(
            yearmonthdate=time.strftime("%Y%m%d", time.localtime(folder_time))
        )
        folder = os.path.join(
            date_folder,
            sub_folder
        )

        # check if folder exists. if it doesnt, create it and zip the old one
        if self._zip_old and not os.path.isdir(date_folder):
            # go back 23h in time
            old_date_folder = self._storage_path.format(
                yearmonthdate=time.strftime("%Y%m%d", time.localtime(folder_time - 60 * 60 * 23))
            )
            if os.path.isdir(old_date_folder):
                thr = threading.Thread(target=zip_it, args=(old_date_folder,), kwargs={})
                thr.start()  # we dont care when it finishes

        # ensure all subfolders exist
        self._dmakedir(folder, exist_ok=True)

        return folder

    def get_storage_path(self, sub_folder, name=None, folder_time=None):
        folder = self._get_storage_path(sub_folder, folder_time)

        if name:
            return os.path.join(
                folder,
                name
            )
        else:
            return folder

    def exists(self,
               sub_folder,
               file_ext=".xml",
               file_name=None,
               folder_time=None):
        file_path = self.get_storage_path(sub_folder, file_name + file_ext, folder_time)
        return self._disfile(file_path)

    def folder_exists(self, sub_folder, folder_time=None):
        folder_path = self.get_storage_path(sub_folder, folder_time=folder_time)
        return self._disfolder(folder_path)

    def save(self,
             sub_folder,
             file_string,
             file_ext=".xml",
             file_name=None,
             folder_time=None):
        if not file_name:
            name = '{timestamp}_{uuid}'.format(
                timestamp=time.strftime("%Y%m%d-%H%M%S"),
                uuid=self._uuidgen())
            fail_if_exists = True
        else:
            name = file_name
            fail_if_exists = False

        name = name + file_ext

        file_path = self.get_storage_path(sub_folder, name, folder_time=folder_time)
        if self._disfile(file_path):
            if fail_if_exists:
                raise Exception("File exists, but shouldnt!")
        else:
            with self._fopen(file_path, 'wt', encoding="utf-8") as file:
                file.write(file_string)
        return name

    def open(self, sub_folder, name):
        file_path = self.get_storage_path(sub_folder, name)
        stream = self._fopen(file_path, 'rb')
        stream_len = os.path.getsize(file_path)

        return stream, stream_len


class ProcessedFileStore(FileStore):
    def __init__(self):
        super(ProcessedFileStore, self).__init__(
            storage_path="dump/c_processed/{yearmonthdate}")
        self._zip_old = True


class IncidentFileStore(FileStore):
    last_written = None

    def __init__(self, storage_path="dump/d_incidents/{yearmonthdate}"):
        super(IncidentFileStore, self).__init__(
            storage_path=storage_path)

    def save(self,
             sub_folder,
             file_string,
             file_ext=".xml",
             file_name=None,
             folder_time=None):
        name = super(IncidentFileStore, self).save(sub_folder,
                                                   file_string,
                                                   file_ext=file_ext,
                                                   file_name=file_name,
                                                   folder_time=folder_time)
        IncidentFileStore.last_written = datestring.date_to_string()
        return name


class CacheFileStore(FileStore):
    def __init__(self):
        super(CacheFileStore, self).__init__(
            storage_path="dump/b_cache/{yearmonthdate}")

