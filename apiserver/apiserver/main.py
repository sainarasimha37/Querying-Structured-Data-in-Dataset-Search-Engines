import asyncio
from datetime import datetime
import itertools
import json
import lazo_index_service
import logging
import os
import prometheus_client
import re
import redis
import socket
import tornado.ioloop
from tornado.routing import Rule, PathMatches, URLSpec
import tornado.httputil
import tornado.web

import duckdb
from minio import Minio
import requests
from io import BytesIO
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
# import threading
# from multiprocessing.pool import ThreadPool
from multiprocess import Process, Queue
import elasticsearch


from datamart_core.common import PrefixedElasticsearch, setup_logging
from datamart_core.objectstore import get_object_store
from datamart_core.prom import PromMeasureRequest
import datamart_profiler

from .augment import Augment, AugmentResult
from .base import BUCKETS, BaseHandler, Application
from .download import DownloadId, Download, Metadata
from .profile import Profile
from .search import Search
from .sessions import SessionNew, SessionGet
from .upload import Upload
# from tornado.web import RequestHandler
from .graceful_shutdown import GracefulHandler

logger = logging.getLogger(__name__)


PROM_LOCATION = PromMeasureRequest(
    count=prometheus_client.Counter(
        'req_location_count',
        "Location search requests",
    ),
    time=prometheus_client.Histogram(
        'req_location_seconds',
        "Location search request time",
        buckets=BUCKETS,
    ),
)
PROM_STATISTICS = PromMeasureRequest(
    count=prometheus_client.Counter(
        'req_statistics_count',
        "Statistics requests",
    ),
    time=prometheus_client.Histogram(
        'req_statistics_seconds',
        "Statistics request time",
        buckets=BUCKETS,
    ),
)
PROM_VERSION = PromMeasureRequest(
    count=prometheus_client.Counter(
        'req_version_count',
        "Version requests",
    ),
    time=prometheus_client.Histogram(
        'req_version_seconds',
        "Version request time",
        buckets=BUCKETS,
    ),
)


def min_or_none(values, *, key=None):
    """Like min(), but returns None if the input is empty.
    """
    values = iter(values)
    try:
        first_value = next(values)
    except StopIteration:
        return None
    return min(itertools.chain((first_value,), values), key=key)


class LocationSearch(BaseHandler):
    @PROM_LOCATION.sync()
    def post(self):
        query = self.get_body_argument('q').strip()
        geo_data = self.application.geo_data
        areas = geo_data.resolve_name_all(query)
        area = min_or_none(areas, key=lambda a: a.type.value)
        if area is not None:
            bounds = area.bounds
            logger.info("Resolved area %r to %r", query, area)
            return self.send_json({'results': [
                {
                    'id': area.id,
                    'name': area.name,
                    'boundingbox': bounds,
                }
            ]})
        else:
            return self.send_json({'results': []})


class Statistics(BaseHandler):
    @PROM_STATISTICS.sync()
    def get(self):
        return self.send_json({
            'recent_discoveries': self.application.recent_discoveries,
            'sources_counts': self.application.sources_counts,
            'custom_fields': self.application.custom_fields,
        })


class Version(BaseHandler):
    @PROM_VERSION.sync()
    def get(self):
        return self.send_json({
            'version': os.environ['DATAMART_VERSION'].lstrip('v'),
            'min_profiler_version': datamart_profiler.__version__,
        })


class DocRedirect(BaseHandler):
    def get(self):
        return self.redirect('https://docs.auctus.vida-nyu.org/rest/')


class Snapshot(BaseHandler):
    _re_filenames = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}\.tar\.gz$')

    _most_recent_filename = None
    _most_recent_time = None

    def get(self, filename):
        object_store = get_object_store()

        if filename == 'index.tar.gz':
            filename = self.most_recent_snapshot(object_store)
        elif not self._re_filenames.match(filename):
            return self.send_error(404)

        return self.redirect(object_store.url('snapshots', filename))

    head = get

    @classmethod
    def most_recent_snapshot(cls, object_store):
        now = datetime.utcnow()
        if (
            cls._most_recent_time is None
            or (now - cls._most_recent_time).total_seconds() > 3600
        ):
            cls._most_recent_filename = max(
                name
                for name in object_store.list_bucket_names('snapshots')
                if cls._re_filenames.match(name)
            )
            cls._most_recent_time = now

        return cls._most_recent_filename


class Health(BaseHandler):
    def get(self):
        if self.application.is_closing:
            self.set_status(503, reason="Shutting down")
            return self.finish('shutting down')
        else:
            return self.finish('ok')

PROM_QUERY = PromMeasureRequest(
    count=prometheus_client.Counter(
        'req_search_count_1',
        "Search requests_1",
    ),
    time=prometheus_client.Histogram(
        'req_search_seconds_1',
        "Search request time_1",
        buckets=BUCKETS,
    ),
)

class QueryDuckDB(BaseHandler, GracefulHandler):
    @PROM_QUERY.sync()
    # @PROM_LOCATION.sync()
    
    
    def post(self):
        type_ = self.request.headers.get('Content-Type', '')
        result = None
        try:
            data = self.request.body
            if type_ == 'application/json':
                data = json.loads(data)

            logger.info("started query")
            # object_name = self.get_query_argument('object_name').strip()
            # bucket_name = self.get_query_argument('bucket_name').strip()
            logger.info("came here 1")
            object_name = data['object_name']
            logger.info("came here 2")
            logger.info("Object name: %s", object_name)
            logger.info("came here 3")
            query = data['query']
            logger.info("came here 4")
            logger.info("Query: %s", query)
            logger.info("came here 5")

            minio_client = self.application.minio

            # Check if object_name exists in s3 bucket
            try:
                is_object = minio_client.stat_object("nvn", object_name)
            except Exception as err:
                is_object = False
            
            logger.info("Bucket name: %s", "nvn")
            


            # if doesn't exist, call the download endpoint and download the file
            if not is_object:
                logger.info("Object doesn't exist in s3")
                try:
                    metadata = self.application.elasticsearch.get(
                        'datasets', object_name
                    )['_source']
                except elasticsearch.NotFoundError:
                    return self.send_error_json(404, "No such dataset")
                materialize = metadata.get('materialize', {})
                response =  requests.get(materialize['direct_url'], allow_redirects=True)

    
                file = BytesIO(response.content)
                # Convert the CSV file to Parquet format
                df = pd.read_csv(file)
                parquet_file = BytesIO()
                pq.write_table(pa.Table.from_pandas(df), parquet_file)
                parquet_file.seek(0)
                minio_client.put_object("nvn", object_name+".parquet", parquet_file, len(parquet_file.getvalue()), content_type="application/parquet")
                print("Uploaded the parquet file to s3")

            # Now query the dataset on duckdb (completed)

            # object_name = self.('object_name').strip()
            # bucket_name = self.get_body_argument('bucket_name').strip()
            


            # query = self.get_body_argument('q').strip()
            duckdb = self.application.duckdb
            # print(f"Query: {query}")
            # logger.info("Query: %s", query)
            object_name = object_name+".parquet"
            # Connect to DuckDB and load Parquet file
            duckdb.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('s3://nvn/{object_name}')")
            # duckdb.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('s3://nvn/userdata1.parquet')")
            # result = duckdb.sql("SELECT * FROM data LIMIT 10").fetchnumpy()
            result = json.loads(duckdb.execute(query).fetchdf().to_json(orient="table"))
            duckdb.execute("DROP TABLE data")
            logger.info("Completed query")
            logger.info("Result: %s", result)
        except Exception as e:
            logger.info(f"Error querying data from S3: {e}")

        # return self.send_json({'results': str(result)}) if result else self.send_json({'results': []})
        # return self.write({'results': result}) if result is not None else self.write({'results': []})
        return self.send_json({'results': result}) if result is not None else self.send_json({'results': []})

        


class CustomErrorHandler(tornado.web.ErrorHandler, BaseHandler):
    pass


class ApiRule(Rule):
    VERSIONS = {'1'}

    def __init__(self, pattern, versions, target, kwargs=None):
        assert isinstance(versions, str)
        assert set(versions).issubset(self.VERSIONS)
        assert pattern[0] == '/'
        matcher = PathMatches(f'/api/v[{versions}]{pattern}')
        super(ApiRule, self).__init__(matcher, target, kwargs)


def make_app(debug=False):
    es = PrefixedElasticsearch()
    host, port = os.environ['REDIS_HOST'].split(':')
    port = int(port)
    redis_client = redis.Redis(host=host, port=port)
    lazo_client = lazo_index_service.LazoIndexClient(
        host=os.environ['LAZO_SERVER_HOST'],
        port=int(os.environ['LAZO_SERVER_PORT'])
    )
    minio_client = Minio('192.168.1.17:8050', access_key='devkey',
               secret_key='devpassword', secure=False)
    
    duckdb_client = duckdb.connect()
    duckdb_client.execute("INSTALL httpfs")
    duckdb_client.execute("LOAD httpfs")
    duckdb_client.execute("SET s3_endpoint='192.168.1.17:8050'")
    duckdb_client.execute("SET s3_use_ssl= false")
    duckdb_client.execute("SET s3_url_style='path'")
    duckdb_client.execute("SET s3_region='us-east-1'")
    duckdb_client.execute("SET s3_access_key_id='devkey'")
    duckdb_client.execute("SET s3_secret_access_key='devpassword'")
    
#return Application(

    app = Application(
        [
            ApiRule('/profile', '1', Profile),
            ApiRule('/profile/fast', '1', Profile, {'fast': True}),
            ApiRule('/search', '1', Search),
            ApiRule('/download/([^/]+)', '1', DownloadId),
            ApiRule('/download', '1', Download),
            ApiRule('/metadata/([^/]+)', '1', Metadata),
            ApiRule('/augment', '1', Augment),
            ApiRule('/augment/([^/]+)', '1', AugmentResult),
            ApiRule('/upload', '1', Upload),
            ApiRule('/session/new', '1', SessionNew),
            ApiRule('/session/([^/]+)', '1', SessionGet),
            ApiRule('/location', '1', LocationSearch),
            ApiRule('/statistics', '1', Statistics),
            ApiRule('/version', '1', Version),
            ApiRule('/queryduck', '1', QueryDuckDB),


            URLSpec(r'/(?:api(?:/(?:v[0-9.]+)?)?)?', DocRedirect),
            URLSpec(r'/snapshot/(.*)', Snapshot),

            URLSpec('/health', Health),
        ],
        debug=debug,
        es=es,
        redis_client=redis_client,
        lazo=lazo_client,
        duckdb=duckdb_client,
        minio=minio_client,
        default_handler_class=CustomErrorHandler,
        default_handler_args={"status_code": 404},
    )
    return app


def main():
    setup_logging()
    debug = os.environ.get('AUCTUS_DEBUG') not in (
        None, '', 'no', 'off', 'false',
    )
    prometheus_client.start_http_server(8000)
    logger.info(
        "Startup: apiserver %s %s",
        os.environ['DATAMART_VERSION'],
        socket.gethostbyname(socket.gethostname()),
    )
    if debug:
        logger.error("Debug mode is ON")

    app = make_app(debug)
    app.listen(8002, xheaders=True, max_buffer_size=2147483648)
    loop = tornado.ioloop.IOLoop.current()
    if debug:
        asyncio.get_event_loop().set_debug(True)
    loop.start()
