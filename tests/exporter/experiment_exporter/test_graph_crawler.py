import logging
from unittest import TestCase

from ingest.api.ingestapi import IngestApi
from ingest.exporter.experiment_exporter.graph_crawler import GraphCrawler
from ingest.exporter.metadata import MetadataResource


logging.disable(logging.CRITICAL)


class GraphCrawlerTest(TestCase):

    def test_crawl(self):
        ingest_url = "https://api.ingest.dev.data.humancellatlas.org"
        ingest_client = IngestApi(url=ingest_url)
        graph_crawler = GraphCrawler(ingest_client)

        process_url = f'{ingest_url}/processes/5d9d5bbc46b31e000890801d'
        process = MetadataResource.from_dict(ingest_client.get_process(process_url))
        experiment_graph = graph_crawler.experiment_graph_for_process(process)
