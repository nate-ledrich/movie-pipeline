from elasticsearch import Elasticsearch

from config.config import Config


class ElasticsearchIndexer:
    def __init__(self):
        self.config = Config()
        self.es = Elasticsearch(
            hosts=[{
                'host': self.config.ELASTICSEARCH_HOST,
                'port': self.config.ELASTICSEARCH_PORT,
                'scheme': 'http'}])

    def index_data_into_elasticsearch(self, formatted_data):
        print("Starting data indexing")

        for table_name, table_data in formatted_data.items():
            print(f"Indexing data for table: {table_name}")

            for transformed_row in table_data:
                try:
                    index_response = self.es.index(index=table_name, body=transformed_row)
                    print(f"Indexed document with ID: {index_response['_id']} in index: {table_name}")
                except Exception as e:
                    print(f"Failed to index document: {e}")
                print("Data indexing completed")
