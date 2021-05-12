#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Hook for Redmine"""
import redminelib.exceptions
from redminelib import Redmine

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class RedmineHook(BaseHook):
    """
    Redmine interaction hook, a Wrapper around Redmine Python library.

    :param redmine_conn_id: reference to a pre-defined Redmine Connection
    :type redmine_conn_id: str
    """

    default_conn_name = 'redmine_default'
    conn_name_attr = "redmine_conn_id"
    hook_name = "Redmine"

    def __init__(self, redmine_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.redmine_conn_id = redmine_conn_id
        self.client = None
        self.get_conn()

    def get_conn(self) -> Redmine:
        if self.redmine_conn_id is None:
            raise AirflowException('Cannot get username and password: Redmine connection id is not provided.')
        conn = self.get_connection(self.redmine_conn_id)
        if not getattr(conn, 'login', None):
            raise AirflowException('Missing login in Redmine connection')
        if not getattr(conn, 'password', None):
            raise AirflowException('Missing password in Redmine connection')

        try:
            self.client = Redmine(conn.get_uri(),
                                  username=conn.login,
                                  password=conn.password)
        except redminelib.exceptions.AuthError as e:
            raise AirflowException(f'Failed to create redmine client, redmine error: {str(e)}')
        return self.client
