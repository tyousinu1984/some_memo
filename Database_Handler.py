import os
from logging import Formatter, StreamHandler, getLogger
from importlib import import_module

import boto3

# ロガー初期化 start ####################################################
_LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG")
_logger = getLogger(__name__)
_logger.setLevel(_LOG_LEVEL)
formatter = Formatter(
    fmt="%(asctime)s:[%(filename)s](%(lineno)s)fn:%(funcName)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")

# stdout
s_handler = StreamHandler()
s_handler.setLevel(_LOG_LEVEL)
s_handler.setFormatter(formatter)
_logger.addHandler(s_handler)


class DataAPIHandler(object):
    def __init__(self, config):
        self.boto3 = import_module("boto3")
        self.resourceArn = config["cluster_arn"]
        self.secretArn = config["secret_arn"]
        self.database = config["database"]
        self.rds_data_client = self.boto3.client('rds-data')

    def execute_query(self, sql, key_list=[]):
        try:
            # for the operations which will change the data,like select
            response = self.rds_data_client.execute_statement(
                resourceArn=self.resourceArn,
                secretArn=self.secretArn,
                database=self.database,
                sql=sql)
            print(response)
            response = response["records"]

            if len(key_list) == 0:
                return response
            else:
                result = []
                for record in response:
                    temp = {}
                    for colum, value in zip(key_list, record):

                        if 'isNull' not in value.keys():
                            temp[colum] = list(value.values())[0]
                        else:
                            temp[colum] = None

                    result.append(temp)
                return result

        except Exception as e:
            raise e

    def execute_no_query(self, sql):
        # for the operations which will change the data,like updata,delete and so on
        transaction_id = self.rds_data_client.begin_transaction(
            resourceArn=self.resourceArn,
            secretArn=self.secretArn,
            database=self.database)
        transaction_id = transaction_id["transactionId"]

        try:

            self.rds_data_client.execute_statement(
                resourceArn=self.resourceArn,
                secretArn=self.secretArn,
                database=self.database,
                transactionId=transaction_id,
                sql=sql)
            self.rds_data_client.commit_transaction(
                resourceArn=self.resourceArn,
                secretArn=self.secretArn,
                transactionId=transaction_id)

        except Exception as e:
            self.rds_data_client.rollback_transaction(
                resourceArn=self.resourceArn,
                secretArn=self.secretArn,
                transactionId=transaction_id)
            raise e

    def close_database(self):
        pass


class MysqlDataHandler(object):
    def __init__(self, config):
        self.mysql_connector = import_module("mysql.connector")
        self._connection = MysqlDataHandler.__get_connection(config)
        self._cursor = self._connection.cursor(dictionary=True)

    @staticmethod
    def __get_connection(self, config):
        try:
            _connection = self.mysql_connector.connector.connect(host=config['host'],
                                                    port=config['port'],
                                                    user=config['user'],
                                                    passwd=config['password'],
                                                    db=config['database'],
                                                    charset=config['charset'])
            _autocommit = _connection.autocommit
            if _autocommit:
                _connection.autocommit(False)
            return _connection
        except Exception as e:
            _logger.error("Error: faild to begin rds transaction.")
            _logger.error(e)
            raise e

    def execute_query(self, sql, key_list=[]):
        # select
        try:
            self._cursor.execute(sql)
            rows = self._cursor.fetchall()
            return rows

        except Exception as e:
            _logger.error("Error: faild to execute sql command.")
            _logger.error(e)
            raise e

    def execute_no_query(self, sql):
        # Create,Insert,Delete,update,drop等
        try:
            self._cursor.execute(sql)
        except Exception as e:
            _logger.error("Error: faild to execute sql command.")
            _logger.error(e)
            raise e

    def rollback(self):
        try:
            self._connection.rollback()
        except Exception as e:
            _logger.error("faild to rollback to rds transaction.")
            _logger.error(e)
            raise e

    def commit(self):
        try:
            self._connection.commit()
        except Exception as e:
            _logger.error("faild to commit rds transaction.")
            _logger.error(e)
            raise e

    def close_database(self):
        if self._cursor is not None:
            self._cursor.close
        if self._cursor is not None:
            self._connection.close()


class RedisDataHandler(object):
    def __init__(self, config):
        self.rediscluster = import_module("rediscluster")
        try:
            self.redis_connect = self.rediscluster.RedisCluster(
                startup_nodes=config["startup_nodes"],
                max_connections=config["max_connection"],
                skip_full_coverage_check=True,
                decode_responses=True)
        except Exception as e:
            _logger.error("faild to get redis connection.")
            _logger.error(e)
            raise e

    def keys(self, pattern='*'):
        try:
            return self.redis_connect.keys(pattern)
        except Exception as e:
            _logger.error("Error: faild to execute redis command.")
            _logger.error(e)
            raise e

    # string
    def set(self, key, value):
        try:
            return self.redis_connect.set(key, value)
        except Exception as e:
            _logger.error("Error: faild to execute redis command.")
            _logger.error(e)
            raise e

    def get(self, key):
        try:
            if isinstance(key, list):
                return self.redis_connect.mget(key)
            else:
                return self.redis_connect.get(key)
        except Exception as e:
            _logger.error("Error: faild to execute redis command.")
            _logger.error(e)
            raise e

    # hash
    def hset(self, name, key, value):
        try:
            return self.redis_connect.hset(name, key, value)
        except Exception as e:
            _logger.error("Error: faild to execute redis command.")
            _logger.error(e)
            raise e

    def hget(self, name, key):
        try:
            return self.redis_connect.hget(name, key)
        except Exception as e:
            _logger.error("Error: faild to execute redis command.")
            _logger.error(e)
            raise e

    def hdel(self, name, key=None):
        try:
            if (key):
                return self.redis_connect.hdel(name, key)
            return self.redis_connect.hdel(name)
        except Exception as e:
            _logger.error("Error: faild to execute redis command.")
            _logger.error(e)
            raise e

    def hmset(self, name, mapping):
        try:
            return self.redis_connect.hmset(name, mapping)
        except Exception as e:
            _logger.error("Error: faild to execute redis command.")
            _logger.error(e)
            raise e

    def hgetall(self, name):
        try:
            return self.redis_connect.hgetall(name)
        except Exception as e:
            _logger.error("Error: faild to execute redis command.")
            _logger.error(e)
            raise e

    # list
    def lrange(self, key, start, end):
        try:
            return self.redis_connect.lrange(key, start, end)
        except Exception as e:
            _logger.error("Error: faild to execute redis command.")
            _logger.error(e)
            raise e


class S3DataHandler(object):
    def __init__(self, config):
        self.s3_client = S3DataHandler.__get_s3_client(config)

    @staticmethod
    def __get_s3_client(config):
        """
        s3への接続情報を取得する。
        @return s3の接続情報
        """
        _logger.info("_get_s3 start")
        region_name = config["region_name"]
        s3_client = boto3.client("s3", region_name=region_name)
        _logger.info("_get_s3 end")
        return s3_client

    # Bucket
    def get_bucket_location(self, bucket):
        try:
            location = self.s3_client.get_bucket_location(
                Bucket=bucket)['LocationConstraint']
            return location
        except Exception as e:
            _logger.error("faild to get file bucket location")
            _logger.error(e)
            raise e

    # Object
    def put_object(self, bucket, key, body):
        try:
            self.s3_client.put_object(Bucket=bucket, Key=key, Body=body,
                                      ContentType="application/pdf")
        except Exception as e:
            _logger.error("faild to put file to S3.")
            _logger.error(e)
            raise e

    def get_object(self, bucket, key):
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            return response
        except Exception as e:
            _logger.error("faild to get file from S3.")
            _logger.error(e)
            raise e

    def upload_s3(self, file_name, bucket, key, **tags):
        try:
            self.s3_client.upload_file(file_name, bucket, key)

            if tags:
                S3DataHandler.put_tagging(self, bucket, key, **tags)
        except Exception as e:
            _logger.error("faild to upload file to S3.")
            _logger.error(e)
            raise e

    def get_s3_object_body(self, bucket, key):
        try:
            s3_object = self.s3_client.get_object(Bucket=bucket, Key=key)
            return s3_object["Body"].read().decode("utf-8")
        except Exception as e:
            _logger.error(f"faild to get file {key} body from {bucket}.")
            _logger.error(e)
            raise e

    # Tag
    def get_object_tagging(self, bucket, key):
        try:
            response = self.s3_client.get_object_tagging(
                Bucket=bucket, Key=key)
            return {tag["Key"]: tag["Value"] for tag in response["TagSet"]}
        except Exception as e:
            _logger.error("faild to get file tag.")
            _logger.error(e)
            raise e

    def put_tagging(self, bucket, key, **tags):
        try:
            tagging = {"TagSet": [{"Key": k, "Value": v}
                                  for k, v in tags.items()]}
            self.s3_client.put_object_tagging(
                Bucket=bucket, Key=key, Tagging=tagging)
        except Exception as e:
            _logger.error("faild to put file tag.")
            _logger.error(e)
            raise e


class DynamodbHandler(object):
    def __init__(self):
        self.dynamodb_resource = DynamodbHandler.__get_dynamodb()

    @staticmethod
    def __get_dynamodb():
        """
        DynamoDBのオブジェクトを取得する。
        @return: DynamoDBオブジェクト
        """
        _logger.info("__get_dynamodb start")
        dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
        _logger.info("__get_dynamodb end")
        return dynamodb

    def get_item(self, table_name, key):
        _logger.info("get_item start")
        try:
            table = self.dynamodb_resource.Table(table_name)
            res = table.get_item(Key=key, ConsistentRead=True)
            if 'Item' in res:
                response = res["Item"]
                return response

        except Exception as e:
            _logger.error("faild to commit rds transaction.")
            _logger.error(e)
            raise e
        finally:
            _logger.info("get_item end")

    def put_items(self, table_name, file_body):
        table = self.dynamodb_resource.Table(table_name)
        with table.batch_writer() as batch:
            for item in file_body:
                batch.put_item(Item=item)

    def create_table(self, table_name, hash_key, hash_type, range_key=None,
                     range_type=None):
        key_schema = [{"AttributeName": hash_key, "KeyType": "HASH"}]
        attribute_definitions = [{"AttributeName": hash_key,
                                  "AttributeType": hash_type}]
        if range_key:
            key_schema.append({"AttributeName": range_key, "KeyType": "RANGE"})
            attribute_definitions.append({"AttributeName": range_key,
                                          "AttributeType": range_type})
        self.dynamodb_resource.create_table(
            TableName=table_name, KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput={"ReadCapacityUnits": 1,
                                   "WriteCapacityUnits": 1})

    def delete_table(self, table_name):
        _logger.info("delete_table start")
        self.dynamodb_resource.Table(table_name).delete()

    def truncate_table(self, table_name):
        _logger.info("truncate_table start")
        table = self.dynamodb_resource.Table(table_name)
        key_names = [x["AttributeName"] for x in table.key_schema]
        items = table.scan()["Items"]
        with table.batch_writer() as batch:
            for item in items:
                key = {k: v for k, v in item.items() if k in key_names}
                batch.delete_item(Key=key)
