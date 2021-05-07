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
"""Hook for Mattermost"""
from typing import Any, Optional

import mattermostdriver

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class MattermostHook(BaseHook):  # noqa
    """
    Creates a Mattermost connection, to be used for calls. Takes both Mattermost API token directly and
    connection that has Mattermost API token. If both supplied, Mattermost API token will be used.
    Exposes also the rest of mattermostdriver.Driver args
    Examples:

    .. code-block:: python

        # Create hook
        mattermost_hook = MattermostHook(token="xxx")
        # or
        mattermost_hook = MattermostHook(mattermost_conn_id="mattermost")

        # Call method from Mattermost SDK (you have to handle errors yourself)
        mattermost_hook.client.posts.create_post({
            'channel_id': '1',
            'message': 'Hello, world!'
        })

    :param base_url: A string representing the Mattermost API base URL.
    :type base_url: str
    :param token: Mattermost API token
    :type token: str
    :param mattermost_conn_id: Mattermost connection id that has Mattermost API token in the password field.
    :type mattermost_conn_id: str
    :param timeout: The maximum number of seconds the client will wait
        to connect and receive a response from Mattermost. Default is 30 seconds.
    :type timeout: int
    """

    def __init__(
        self,
        url: str,
        token: Optional[str] = None,
        mattermost_conn_id: Optional[str] = None,
        **client_args: Any,
    ) -> None:
        super().__init__()
        self.url = url
        self.token = self.__get_token(token, mattermost_conn_id)

        if 'login_id' in client_args:
            client_args['login'] = client_args['login_id']
            del client_args['login_id']

        self.client = mattermostdriver.Driver({
            'url': self.url,
            'token': self.token,
            **client_args
        })
        self.client.login()

    def __get_token(self, token: Any, mattermost_conn_id: Any) -> str:
        if token is not None:
            return token

        if mattermost_conn_id is not None:
            conn = self.get_connection(mattermost_conn_id)

            if not getattr(conn, 'password', None):
                raise AirflowException('Missing token(password) in Mattermost connection')
            return conn.password

        raise AirflowException('Cannot get token: No valid Mattermost token nor mattermost_conn_id supplied.')

