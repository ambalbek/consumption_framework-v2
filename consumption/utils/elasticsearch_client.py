import os

try:
    from elastic_transport import RequestsHttpNode as _BaseHttpNode

    def _make_custom_node():
        class CustomHttpNode(_BaseHttpNode):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                # Get the proxies from HTTP_PROXY and HTTPS_PROXY environment variables
                self.session.proxies = {
                    "http": os.environ.get("HTTP_PROXY", os.environ.get("http_proxy")),
                    "https": os.environ.get("HTTPS_PROXY", os.environ.get("https_proxy")),
                }

        return CustomHttpNode

except ImportError:
    # elastic_transport >= 9 removed RequestsHttpNode; fall back to Urllib3HttpNode.
    # urllib3 picks up HTTP_PROXY / HTTPS_PROXY automatically via ProxyManager.
    from elastic_transport import Urllib3HttpNode as _BaseHttpNode

    def _make_custom_node():
        return _BaseHttpNode

from elasticsearch import Elasticsearch


class ElasticsearchClient(Elasticsearch):
    """
    This extends the Elasticsearch client to provide proxy support via a custom HTTP node.
    """

    CustomHttpNode = _make_custom_node()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, node_class=self.CustomHttpNode)
