# Copyright 2013-2016 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Implementation of the DocManager interface.

Receives documents from an OplogThread and takes the appropriate actions on
the defined HTTP endpoint.
"""
import base64
import logging
import httplib
import json
import util
import time

from datetime import datetime
from threading import Timer
from bson import json_util

from mongo_connector import errors
from mongo_connector.compat import u
from mongo_connector.constants import (DEFAULT_COMMIT_INTERVAL,
                                       DEFAULT_MAX_BULK)
from mongo_connector.util import exception_wrapper, retry_until_ok
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter

wrap_exceptions = exception_wrapper({    })

LOG = logging.getLogger(__name__)

class DocManager(DocManagerBase):
    """Implementation of the DocManager interface.
    Receives documents from an OplogThread and sends updates to Endpoint.
    """

    def __init__(self, url, chunk_size, auto_commit_interval=DEFAULT_COMMIT_INTERVAL, unique_key='_id', **kwargs):
 
        self.unique_key = unique_key
        self.url = url
        self.connection = httplib.HTTPConnection(self.url)
        self.headers = {'Content-type': 'application/json'}

        self.auto_commit_interval = auto_commit_interval
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        self._formatter = DefaultDocumentFormatter()

        self.has_attachment_mapping = False

    def stop(self):
        """Stop the auto-commit thread."""
        self.connection.close()
        self.auto_commit_interval = None

    def apply_update(self, doc, update_spec):
        if "$set" not in update_spec and "$unset" not in update_spec:
            # Don't try to add ns and _ts fields back in from doc
            return update_spec
        return super(DocManager, self).apply_update(doc, update_spec)

    @wrap_exceptions
    def update(self, document_id, update_spec, namespace, timestamp):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.
        """
        
        message = {
        'action' : 'U',
        '_id' : str(document_id),
        '_ts' : timestamp,
        'body' : update_spec
        }

        json_message = self.serialize_to_json(message)
        self.connection.connect()
        self.connection.request('POST', '/loglistener/api/log', json_message, self.headers)
        response = self.connection.getresponse()
        if response.status == 500:
            LOG.info(response.msg)
        r = response.read()
        self.connection.close()

        # self.commit()
        # updated = self.apply_update(document, update_spec)
        # self.upsert(updated, namespace, timestamp)
        # return updated

    def _doc_to_json(self, doc = None, _id, action, timestamp)
        message = {
        'action' : action,
        '_ts' : timestamp,
        '_id' : _id,
        'body' : doc
        }
        return json.dumps(message, default=json_util.default)

    def _send_upsert(self, json)
        self.connection.connect()
        self.connection.request('POST', '/loglistener/api/log', json, self.headers)
        response = self.connection.getresponse()
        r = response.read()
        self.connection.close()

    @wrap_exceptions
    def upsert(self, doc, namespace, timestamp):
        json_message = self._doc_to_json(doc, str(doc[self.unique_key]), 'C', timestamp)
        self.connection.connect()
        self.connection.request('POST', '/loglistener/api/log', json_message, self.headers)
        response = self.connection.getresponse()
        r = response.read()
        self.connection.close()

    @wrap_exceptions
    def bulk_upsert(self, docs, namespace, timestamp)
        jsonmessages = []
        jsondocs = (self._doc_to_json(d, namespace, timestamp) for d in docs)
        if self.chunk_size > 0:
            batch = list(next(jsondocs) for i in range(self.chunk_size))
            while batch:
                jsonmessages = []
                jsonmessages.append(batch)
                self._send_upsert(jsonmessages)
                batch = list(next(jsondocs) for i in range(self.chunk_size))
        else:
            self._send_upsert(jsondocs)

    @wrap_exceptions
    def remove(self, document_id, namespace, timestamp):
        json_message = self._doc_to_json(None, document_id, 'D', timestamp)
        self.connection.connect()
        self.connection.request('POST', '/loglistener/api/log', json_message, self.headers)
        response = self.connection.getresponse()
        r = response.read()
        self.connection.close()

    def commit(self):
        pass

    def search(self, start_ts, end_ts):
        pass

    @wrap_exceptions
    def get_last_doc(self):
        """Get the most recently modified document timestamp from endpoint.
        """
        self.connection.connect()
        self.connection.request('GET', '/loglistener/api/log/max-touched')
        response = self.connection.getresponse()
        r = response.read()
        dict = json.loads(r)
        self.connection.close()
        if dict['_ts'] == -1:
            return None
        else:
            return dict
