#!/usr/bin/env python3
import os

import aws_cdk as cdk
from aws_cdk import Stack
from constructs import Construct

from infrastructure.eventbridge_pipes import UserEventsPipeline


app = cdk.App()


class MyApplicationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        UserEventsPipeline(self, "UserEvents", event_source_name="myapp.users")


stack = MyApplicationStack(app, "EventBridgePipesDynamoDBToEventBridgeCDK")

app.synth()
