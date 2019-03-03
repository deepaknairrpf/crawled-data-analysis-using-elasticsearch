import gzip
import json

from elasticsearch import Elasticsearch
from elasticsearch import helpers


def yield_json_objs_from_file(file_name):

    with gzip.open(file_name, 'r') as fin:
        for line in fin:
            data_json = json.loads(line.decode('utf-8'))
            yield data_json


class CrawledData:

    def __init__(self, category, crawl_date, subcategory, title, mrp, urlh, http_status, pack_size, available_price):
        self.category = category
        self.crawl_date = crawl_date
        self.subcategory = subcategory
        self.title = title
        self.mrp = mrp
        self.urlh = urlh
        self.http_status = http_status
        self.pack_size = pack_size
        self.available_price = available_price

    @staticmethod
    def build_object(json_item):
        crawled_data = CrawledData(
            category=json_item['category'],
            crawl_date=json_item['crawl_date'],
            subcategory=json_item['subcategory'],
            title=json_item['title'],
            mrp=float(json_item['mrp'] or 0),
            urlh=json_item['urlh'],
            http_status=json_item['http_status'],
            pack_size=json_item['pack_size'],
            available_price=float(json_item['available_price'] or 0),
        )
        return crawled_data


class ElasticsearchDataExporter:

    @staticmethod
    def export_data(es_client, file_name, index, doc_type, chunk_size=100, stop_after=None):
        objs = []
        objs_posted = 0
        obj_successful = 0

        for json_item in yield_json_objs_from_file(file_name):
            urlh = json_item['urlh']
            action = {
                "_id": urlh,
                "_source": json_item
            }
            objs.append(action)

            if len(objs) == chunk_size:
                success, failed = helpers.bulk(es_client, objs, index=index, doc_type=doc_type)

                if success != len(objs):
                    print("Bulk create failed..Please re-run.")
                    break
                else:
                    objs_posted += len(objs)
                    obj_successful += success
                    objs = []

            if stop_after is not None and objs_posted >= stop_after:
                break

        return obj_successful


if __name__ == "__main__":
    es = Elasticsearch()
    yesterday_index = "yesterdays-crawled-data-index"
    today_index = "todays-crawled-data-index"
    doc_type = "retail-crawled-data"

    todays_obj_successful = ElasticsearchDataExporter.export_data(es, "t.json.gz", today_index, doc_type)
    yesterdays_obj_successful = ElasticsearchDataExporter.export_data(es, "y.json.gz", yesterday_index, doc_type)
    print(todays_obj_successful, yesterdays_obj_successful)
