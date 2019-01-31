import boto3
from enum import Enum, auto
from datetime import date, datetime
from abc import ABC, abstractmethod


class StackLaunchException(Exception): pass


class TemplateValidationException(Exception): pass


class TimeoutException(Exception): pass


class Service(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name.lower()

    S3 = auto()
    IAM = auto()
    KMS = auto()
    LAMBDA = auto()
    STEPFUNCTIONS = auto()
    CLOUDFORMATION = auto()


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class BotoClientFactory(object):
    """
    Holds and serves boto3 client singletons
    """
    clients = {
        Service.S3: boto3.client(Service.S3.value),
        Service.IAM: boto3.client(Service.IAM.value),
        Service.KMS: boto3.client(Service.KMS.value),
        Service.LAMBDA: boto3.client(Service.LAMBDA.value),
        Service.STEPFUNCTIONS: boto3.client(Service.STEPFUNCTIONS.value),
        Service.CLOUDFORMATION: boto3.client(Service.CLOUDFORMATION.value)
    }

    @staticmethod
    def client_for(service_namespace):
        if not service_namespace in Service:
            raise ValueError('Unsupported service_namespace: {}'.format(service_namespace))
        else:
            return BotoClientFactory.clients[service_namespace]


class BotoResourceFactory(object):
    """
    Holds and serves boto3 resource API singletons
    """
    resources = {
        Service.CLOUDFORMATION: boto3.resource(Service.CLOUDFORMATION.value)
    }

    @staticmethod
    def resource_for(service_namespace):
        if not service_namespace in Service:
            raise ValueError('Unsupported service_namespace: {}'.format(service_namespace))
        else:
            return BotoResourceFactory.resources[service_namespace]


class Command(ABC):

    @abstractmethod
    def execute(self):
        pass
