from typing import Optional

from airflow.models import BaseOperator
from airflow.providers.mattermost.hooks.mattermost import MattermostHook
from airflow.utils.decorators import apply_defaults


class MattermostAPIOperator(BaseOperator):
    """
    Base Mattermost Operator
    Only one of `mattermost_conn_id` and `token` is required.

    :param mattermost_conn_id: Mattermost connection id which its password is Mattermost API token. Optional
    :type mattermost_conn_id: str
    :param token: Mattermost API token. Optional
    :type token: str
    """

    @apply_defaults
    def __init__(
        self,
        *,
        mattermost_conn_id: Optional[str] = None,
        token: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.token = token  # type: Optional[str]
        self.mattermost_conn_id = mattermost_conn_id  # type: Optional[str]

    def execute(self, **kwargs):  # noqa: D403
        """
        MattermostAPIOperator calls will not fail even if the call is not unsuccessful.
        It should not prevent a DAG from completing in success
        """
        raise NotImplementedError(
            "MattermostAPIOperator should not be used directly. Chose one of the subclasses instead"
        )


class MattermostCreatePostOperator(MattermostAPIOperator):
    """
    Posts messages to a mattermost channel
    Examples:

    .. code-block:: python

        mattermost = MattermostCreatePostOperator(
            task_id="post_hello",
            dag=dag,
            token="XXX",
            text="hello there!",
            channel="123",
        )

    :param channel: channel id in which to post message. (templated)
    :type channel: str
    :param text: message to send to mattermost. (templated)
    :type text: str
    """

    template_fields = ('text', 'channel')

    @apply_defaults
    def __init__(
        self,
        channel: str = '#general',
        text: str = 'No message has been set.\n'
        'Here is a cat video instead\n'
        'https://www.youtube.com/watch?v=J---aiyznGQ',
        root_id: Optional[int] = None,
        **kwargs,
    ) -> None:
        self.channel = channel
        self.text = text
        self.root_id = root_id

        super().__init__(**kwargs)

    def execute(self, **kwargs):
        """
        MattermostCreatePostOperator calls will not fail even if the call is not
        unsuccessful. It should not prevent a DAG from completing in success
        """
        mattermost = MattermostHook(token=self.token,
                                    mattermost_conn_id=self.mattermost_conn_id)
        mattermost.client.posts.create_post({
            'channel_id': self.channel,
            'message': self.text,
            'root_id': self.root_id
        })
