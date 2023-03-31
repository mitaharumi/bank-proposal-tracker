from access import *
import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel


class DynamodbSettings(BaseModel):
    aws_access_key_id: str = AWS_ACCESS_KEY_ID
    aws_secret_access_key: str = AWS_SECRET_ACCESS_KEY
    region: str = 'sa-east-1'
    table: str = 'bank-proposals'


class Dynamodb:
    def __init__(self, settings: DynamodbSettings):
        self.dynamodb_settings = settings
        self.dynamodb: boto3.resources.factory.dynamodb.ServiceResource = self.connect()

    def connect(self) -> boto3.resources.base.ServiceResource:
        try:
            connection = boto3.resource(
                'dynamodb',
                region_name=self.dynamodb_settings.region,
                aws_access_key_id=self.dynamodb_settings.aws_access_key_id,
                aws_secret_access_key=self.dynamodb_settings.aws_secret_access_key)
        except Exception:
            raise Exception("dynamodb connection error")
        else:
            return connection

    def show_all_tables(self):
        print(list(self.dynamodb.tables.all()))

    def show_all_data_from_resource(self):
        print(list(self.dynamodb.describe_table()))

    def create_table(self, key_schema, attribute_definitions):
        try:
            table = {
                'TableName': self.dynamodb_settings.table,
                'KeySchema': [{'AttributeName': name, 'KeyType': type} for name, type in key_schema],
                'AttributeDefinitions': [{'AttributeName': name, 'AttributeType': type} for name, type in attribute_definitions],
                'ProvisionedThroughput': {'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
            }

            self.dynamodb.create_table(**table)
            print(f'table {self.dynamodb_settings.table} created')

        except ClientError as e:
            raise ValueError(f'table {self.dynamodb_settings.table} already exists') if e.response['Error']['Code'] == 'ResourceInUseException' else e


db = Dynamodb(DynamodbSettings())

# db.create_table([('id', 'HASH')], [('id', 'S')])

db.show_all_data_from_resource()