import json
from aws_cdk import (
    Stack,
    aws_pipes as pipes,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
)
from constructs import Construct


class UserEventsPipeline(Construct):
    """
    A CDK Stack that creates EventBridge Pipes to handle DynamoDB Stream events
    and publishes them to the default event bus.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        event_source_name: str = "myapp.users",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        """
        Initialize the stack.
        
        Args:
            scope: The scope in which to define this construct.
            construct_id: The scoped construct ID.
            event_source_name: The source name for EventBridge events.
        """

        ddb = dynamodb.Table(
            self,
            "DDBTable",
            partition_key=dynamodb.Attribute(
                name="PK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(name="SK", type=dynamodb.AttributeType.STRING),
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
        )

        pipe_iam_role = self.create_pipe_role(ddb_table=ddb)

        # User Deleted Pipe
        self.create_pipe(
            "UserDeleted",
            "REMOVE",
            '{"userId": <$.dynamodb.Keys.PK.S>}',
            ddb,
            pipe_iam_role,
            event_source_name,
        )
        self.create_pipe(
            "UserCreated",
            "INSERT",
            '{"userId": <$.dynamodb.Keys.PK.S>}',
            ddb,
            pipe_iam_role,
            event_source_name,
        )
        self.create_pipe(
            "UserModified",
            "MODIFY",
            '{"userId": <$.dynamodb.Keys.PK.S>, "oldImage": <$.dynamodb.OldImage>, "newImage": <$.dynamodb.NewImage>}',
            ddb,
            pipe_iam_role,
            event_source_name,
        )

    def create_pipe(
        self,
        operation_type: str,
        event_name: str,
        input_template: str,
        ddb_table: dynamodb.Table,
        role: iam.Role,
        event_source_name: str,
    ) -> pipes.CfnPipe:
        """Helper method to create EventBridge Pipes with common configuration."""
        self._validate_pipe_parameters(ddb_table, event_name)
        source_params = self._create_source_parameters(event_name)
        target_params = self._create_target_parameters(
            operation_type, input_template, event_source_name
        )

        return pipes.CfnPipe(
            self,
            f"{operation_type}Pipe",
            role_arn=role.role_arn,
            source=ddb_table.table_stream_arn,
            source_parameters=source_params,
            target=f"arn:{Stack.of(self).partition}:events:{Stack.of(self).region}:{Stack.of(self).account}:event-bus/default",
            target_parameters=target_params,
        )

    def _validate_pipe_parameters(self, ddb_table: dynamodb.Table, event_name: str):
        if not ddb_table.table_stream_arn:
            raise ValueError(
                f"DynamoDB table '{ddb_table.table_name}' must have streams enabled"
            )
        if event_name not in ["INSERT", "MODIFY", "REMOVE"]:
            raise ValueError(
                f"event_name must be one of: INSERT, MODIFY, REMOVE. Got: '{event_name}'"
            )

    def _create_source_parameters(
        self, event_name: str
    ) -> pipes.CfnPipe.PipeSourceParametersProperty:
        key_prefix = "USER#"
        batch_size = 1
        starting_position = "LATEST"

        return pipes.CfnPipe.PipeSourceParametersProperty(
            dynamo_db_stream_parameters=pipes.CfnPipe.PipeSourceDynamoDBStreamParametersProperty(
                starting_position=starting_position, batch_size=batch_size
            ),
            filter_criteria=pipes.CfnPipe.FilterCriteriaProperty(
                filters=[
                    pipes.CfnPipe.FilterProperty(
                        pattern=json.dumps(
                            {
                                "eventName": [event_name],
                                "dynamodb": {
                                    "Keys": {"PK": {"S": [{"prefix": key_prefix}]}}
                                },
                            }
                        )
                    )
                ]
            ),
        )

    def _create_target_parameters(
        self, operation_type: str, input_template: str, event_source_name: str
    ) -> pipes.CfnPipe.PipeTargetParametersProperty:
        return pipes.CfnPipe.PipeTargetParametersProperty(
            input_template=input_template,
            event_bridge_event_bus_parameters=pipes.CfnPipe.PipeTargetEventBridgeEventBusParametersProperty(
                detail_type=operation_type, source=event_source_name
            ),
        )

    def create_pipe_role(self, ddb_table: dynamodb.Table) -> iam.Role:
        """Create IAM role for EventBridge Pipes with necessary permissions."""
        return iam.Role(
            self,
            "PipeIAMRole",
            assumed_by=iam.ServicePrincipal("pipes.amazonaws.com"),
            description="Role for EventBridge Pipes",
            inline_policies={
                "SourcePolicy": self._create_source_policy(ddb_table),
                "TargetPolicy": self._create_target_policy(),
            },
        )

    def _create_source_policy(self, ddb_table: dynamodb.Table) -> iam.PolicyDocument:
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "dynamodb:DescribeStream",
                        "dynamodb:GetRecords",
                        "dynamodb:GetShardIterator",
                        "dynamodb:ListStreams",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[ddb_table.table_stream_arn],
                )
            ]
        )

    def _create_target_policy(self) -> iam.PolicyDocument:
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=["events:PutEvents"],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:{Stack.of(self).partition}:events:{Stack.of(self).region}:{Stack.of(self).account}:event-bus/default"
                    ],
                )
            ]
        )
