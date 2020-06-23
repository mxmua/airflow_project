import statsd


class ProjStatsdClient:
    def __init__(self, host, port, user_id='mxmua', dag_id='air101_project_with_parts'):
        self.gauge_prefix = f'airflow101.{user_id}.{dag_id}'
        self._client = statsd.StatsClient(host, port)

        print(f'SC init {host}:{port}')

    def flat_value(self, context, value=0):
        self._client.gauge(f'{self.gauge_prefix}.{context}', value)

        print(f'Sending metric {self.gauge_prefix}.{context} value: {value}')