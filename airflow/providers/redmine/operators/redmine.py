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
from typing import Any, Dict, Union

from airflow.models import BaseOperator
from airflow.providers.redmine.hooks.redmine import RedmineHook
from airflow.utils.decorators import apply_defaults


class RedmineApiOperator(BaseOperator):
    """
    RedmineApiOperator to interact and perform action on Redmine issue tracking system.
    This operator is designed to use Redmine Python SDK: https://python-redmine.com/

    :param redmine_conn_id: reference to a pre-defined Redmine Connection
    :type redmine_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        *,
        redmine_conn_id: str = 'redmine_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.redmine_conn_id = redmine_conn_id

    def execute(self, context: Dict) -> Any:
        """
        RedmineApiOperator calls will not fail even if the call is not unsuccessful.
        It should not prevent a DAG from completing in success
        """
        raise NotImplementedError(
            "RedmineApiOperator should not be used directly. Chose one of the subclasses instead"
        )


class RedmineCreateIssueOperator(RedmineApiOperator):
    """
    Create issue in Redmine project
    Examples:
    .. code-block:: python
        mattermost = RedmineCreateIssueOperator(
            dag=dag,
            redmine_conn_id='redmine_default',
            project_id=1,
            subject='Test',
            description='Custom description',
        )
    :param project_id: id or identifier of issue’s project.
    :type project_id: str
    :param subject: issue subject. (templated)
    :type subject: str
    """

    template_fields = ('subject', 'desc')

    @apply_defaults
    def __init__(
        self,
        project_id: Union[str, int],
        subject: str,
        **client_args
    ) -> None:
        self.project_id = project_id
        self.subject = subject
        self.client_args = client_args

        super().__init__(**client_args)

        if 'dag' in self.client_args:
            del self.client_args['dag']
        if 'redmine_conn_id' in self.client_args:
            del self.client_args['redmine_conn_id']

    def execute(self, **kwargs):
        redmine = RedmineHook(redmine_conn_id=self.redmine_conn_id).client
        redmine.issue.create(project_id=self.project_id, **self.client_args)
