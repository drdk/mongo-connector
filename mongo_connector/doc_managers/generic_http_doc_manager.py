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
#import http.client
import json

from threading import Timer

import bson.json_util

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

    def __init__(self, url, auto_commit_interval=DEFAULT_COMMIT_INTERVAL, unique_key='_id', **kwargs):
 
        self.unique_key = unique_key
        self.url = url
        #self.connection = http.client.HTTPSConnection(self.url)
        self.headers = {'Content-type': 'application/json'}

        self.auto_commit_interval = auto_commit_interval
        self.meta_index_name = meta_index_name
        self.meta_type = meta_type
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        if self.auto_commit_interval not in [None, 0]:
            self.run_auto_commit()
        self._formatter = DefaultDocumentFormatter()

        self.has_attachment_mapping = False
        self.attachment_field = attachment_field

    def stop(self):
        """Stop the auto-commit thread."""
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
        self.commit()
        updated = self.apply_update(document, update_spec)
        self.upsert(updated, namespace, timestamp)
        return updated

    @wrap_exceptions
    def upsert(self, doc, namespace, timestamp):
       message = {
        'action' : 'CU',
        'body' : doc
    }

    json_message = json.dumps(message)
    LOG.info('upsert on ' + doc[self.unique_key] + ' called')
    # self.connection.request('POST', '/markdown', json_message, self.headers)

    @wrap_exceptions
    def remove(self, document_id, namespace, timestamp):
        message = {
        'action' : 'D',
        'deleted_id' : document_id
    }

    json_message = json.dumps(message)
    LOG.info('remove on ' + document_id + ' called')
    # self.connection.request('POST', '/markdown', json_message, self.headers)

    def commit(self):
        """ Performs a commit
        """
        return

    @wrap_exceptions
    def get_last_doc(self):
        """Get the most recently modified document from endpoint.
        right now, we do not have an endpoint for this, so just return None"""

        return None