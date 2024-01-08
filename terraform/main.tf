terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region  = "eu-west-2"
}

resource "aws_security_group" "c9_velo_securitygroup" {
  name        = "c9_velo_securitygroup"
  description = "Allow TLS inbound traffic"
  vpc_id      = "vpc-04423dbb18410aece"

  ingress {
    description      = "TLS from VPC"
    from_port        = 5432
    to_port          = 5432
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"] 
  }

  tags = {
    Name = "c9_velo_securitygroup"
  }
}

resource "aws_db_instance" "c9_velo_deloton" {
  allocated_storage = 10
  identifier = "c9-velo-deloton"
  publicly_accessible = true
  engine = "postgres"
  engine_version = "15.3"
  instance_class = "db.t3.micro"
  username = var.DATABASE_USERNAME
  password = var.DATABASE_PASSWORD
  skip_final_snapshot = true 
  availability_zone = "eu-west-2b"
  performance_insights_enabled = false

  db_subnet_group_name = "public_subnet_group"
  
  vpc_security_group_ids = [aws_security_group.c9_velo_securitygroup.id]
}


# ECR for Daily Report

resource "aws_ecr_repository" "c9_deloton_daily_report_t" {
  name                 = "c9-deloton-daily-report-t"
  image_tag_mutability = "MUTABLE"
}


# Report: Lambda Role and Permissions

resource "aws_iam_role" "c9_deloton_lambda_report_role" {
name   = "c9-deloton-lambda-report-role"
assume_role_policy = <<EOF
{
 "Version": "2012-10-17",
 "Statement": [
   {
     "Action": "sts:AssumeRole",
     "Principal": {
       "Service": "lambda.amazonaws.com"
     },
     "Effect": "Allow",
     "Sid": ""
   }
 ]
}
EOF
}

resource "aws_iam_policy" "iam_policy_for_lambda" {
 
 name         = "aws_iam_policy_for_terraform_aws_lambda_role"
 path         = "/"
 description  = "AWS IAM Policy for managing aws lambda role"
 policy = <<EOF
{
 "Version": "2012-10-17",
 "Statement": [
   {
     "Action": [
       "logs:CreateLogGroup",
       "logs:CreateLogStream",
       "logs:PutLogEvents"
     ],
     "Resource": "arn:aws:logs:*:*:*",
     "Effect": "Allow"
   }
 ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "attach_iam_policy_to_iam_role" {
 role        = aws_iam_role.c9_deloton_lambda_report_role.name
 policy_arn  = aws_iam_policy.iam_policy_for_lambda.arn
}


#Report : Lambda 

resource "aws_lambda_function" "c9-deloton-lambda-report-t" {
    function_name = "c9-deloton-lambda-report-t"
    role = aws_iam_role.c9_deloton_lambda_report_role.arn
    image_uri = "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c9-deloton-daily-report-t:latest"
    package_type  = "Image"
    timeout = 15
    environment {
      variables = {
      DATABASE_IP = "${var.DATABASE_IP}",
      DATABASE_NAME ="${var.DATABASE_NAME}",
      DATABASE_PASSWORD = "${var.DATABASE_PASSWORD}",
      DATABASE_PORT ="${var.DATABASE_PORT}",
      DATABASE_USERNAME = "${var.DATABASE_USERNAME}",
      AWS_ACCESS_KEY_ID_ = "${var.AWS_ACCESS_KEY_ID_}",
      AWS_SECRET_ACCESS_KEY_ = "${var.AWS_SECRET_ACCESS_KEY_}"
    }
}
}


# Report : Step Function Permissions

resource "aws_iam_role" "iam_for_sfn" {
  name = "stepFunctionSampleStepFunctionExecutionIAM"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "states.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_policy" "policy_publish_ses" {
  name        = "stepFunctionSampleSESInvocationPolicy"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
              "SES:SendEmail"
            ],
            "Resource": "*"
        }
    ]
}
EOF
}

resource "aws_iam_policy" "policy_invoke_lambda" {
  name        = "stepFunctionSampleLambdaFunctionInvocationPolicy"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction",
                "lambda:InvokeAsync"
            ],
            "Resource": "*"
        }
    ]
}
EOF
}

// Attach policy to IAM Role for Step Function
resource "aws_iam_role_policy_attachment" "iam_for_sfn_attach_policy_invoke_lambda" {
  role       = "${aws_iam_role.iam_for_sfn.name}"
  policy_arn = "${aws_iam_policy.policy_invoke_lambda.arn}"
}

resource "aws_iam_role_policy_attachment" "iam_for_sfn_attach_policy_publish_ses" {
  role       = "${aws_iam_role.iam_for_sfn.name}"
  policy_arn = "${aws_iam_policy.policy_publish_ses.arn}"
}



#Report: Step function


resource "aws_sfn_state_machine" "c9_deloton_report_fsm_t" {
  name     = "c9-deloton-report-fsm-t"
  role_arn = "${aws_iam_role.iam_for_sfn.arn}"

  definition = <<EOF
{
  "Comment": "A description of my state machine",
  "StartAt": "Lambda Invoke",
  "States": {
    "Lambda Invoke": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName":  "${aws_lambda_function.c9-deloton-lambda-report-t.arn}"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Next": "SendEmail"
    },
    "SendEmail": {
      "Type": "Task",
      "End": true,
      "Parameters": {
        "Content": {
          "Simple": {
            "Body": {
              "Html": {
                "Data.$": "$.body"
              }
            },
            "Subject": {
              "Data": "Daily Report"
            }
          }
        },
        "Destination": {
          "ToAddresses": [
            "trainee.charlie.dean@sigmalabs.co.uk"
          ]
        },
        "FromEmailAddress": "trainee.charlie.dean@sigmalabs.co.uk"
      },
      "Resource": "arn:aws:states:::aws-sdk:sesv2:sendEmail"
    }
  }
}
EOF
}

# Report: EventBridge scheduler roles and permissions


# Create a role to attach the policy to
resource "aws_iam_role" "iam_for_sfn_2" {
  name = "stepFunctionSampleStepFunctionExecutionIAM_2"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "states.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    },
    {
      "Effect": "Allow",
      "Principal": {
          "Service": "scheduler.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
      }
    ]
}
EOF
}

## Attaching a step function to the schedule ##

# Create a resource that allows running step functions
resource "aws_iam_policy" "step-function-policy" {
    name = "ExecuteStepFunctions_Charlie"
    policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "states:StartExecution"
            ],
            "Resource": [
                aws_sfn_state_machine.c9_deloton_report_fsm_t.arn
            ]
        }
    ]
})
}

# Report : EventBridge Schedule

# Attach the policy to the role
resource "aws_iam_role_policy_attachment" "attach-execution-policy" {
  role       = aws_iam_role.iam_for_sfn_2.name
  policy_arn = aws_iam_policy.step-function-policy.arn
}

resource "aws_scheduler_schedule" "c9_deloton_report_schedule_t" {
  name        = "c9-deloton-report-schedule-t"
  group_name  = "default"

  flexible_time_window {
    maximum_window_in_minutes = 15
    mode = "FLEXIBLE"
  }
  schedule_expression_timezone = "Europe/London"
  schedule_expression = "cron(30 09 * * ? *)" 

  target{
    arn = aws_sfn_state_machine.c9_deloton_report_fsm_t.arn
    role_arn = aws_iam_role.iam_for_sfn_2.arn
    input = jsonencode({
      Payload = "Hello, ServerlessLand!"
    })
  }
}


# ECR Repository for pipeline Docker image
resource "aws_ecr_repository" "pipeline_ecr_repo" {
  name = "c9-deloton-pipeline-repo"
}

# My account for reference
data "aws_caller_identity" "current" {}

# Existing cohort 9 cluster, here for reference
data "aws_ecs_cluster" "existing_cluster" {
  cluster_name = "c9-ecs-cluster"
}

# Logging messages
resource "aws_cloudwatch_log_group" "ecs_log_group" {
  name = "c9-deloton-log"
}

# Task definition for pipeline service
resource "aws_ecs_task_definition" "pipeline_task_def" {
  family = "c9-deloton-pipeline-task-def"
  network_mode = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu = 1024
  memory = 3072
  execution_role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/ecsTaskExecutionRole"
  container_definitions = jsonencode([{
    name = "c9-deloton-pipeline-container"
    image = "${aws_ecr_repository.pipeline_ecr_repo.repository_url}"
    essential = true
    logConfiguration = {
    logDriver = "awslogs"
    options = {
        awslogs-group = aws_cloudwatch_log_group.ecs_log_group.name
        awslogs-region = "eu-west-2"
        awslogs-stream-prefix = "ecs"
      }
      }
    environment = [
      {
        name = "DATABASE_USERNAME"
        value = var.DATABASE_USERNAME
      },
      {
        name = "DATABASE_PASSWORD"
        value = var.DATABASE_PASSWORD
      },
      {
        name = "AWS_ACCESS_KEY_ID_"
        value = var.AWS_ACCESS_KEY_ID_
      },
      {
        name = "AWS_SECRET_ACCESS_KEY_"
        value = var.AWS_SECRET_ACCESS_KEY_
      },
      {
        name = "DATABASE_IP"
        value = var.DATABASE_IP
      },
      {
        name = "DATABASE_PORT"
        value = var.DATABASE_PORT
      },
      {
        name = "DATABASE_NAME"
        value = var.DATABASE_NAME
      },
      {
        name = "KAFKA_TOPIC"
        value = var.KAFKA_TOPIC
      },
      {
        name = "BOOTSTRAP_SERVERS"
        value = var.BOOTSTRAP_SERVERS
      },
      {
        name = "SECURITY_PROTOCOL"
        value = var.SECURITY_PROTOCOL
      },
      {
        name = "SASL_MECHANISM"
        value = var.SASL_MECHANISM
      },
      {
        name = "USERNAME"
        value = var.USERNAME
      },
      {
        name = "PASSWORD"
        value = var.PASSWORD
      },
      {
        name = "BUCKET_NAME"
        value = var.BUCKET_NAME
      }
    ]
  }])
}

# Pipeline service associated with the task definition
resource "aws_ecs_service" "pipeline_service" {
  name = "c9-deloton-pipeline-service"
  cluster = data.aws_ecs_cluster.existing_cluster.id
  task_definition = aws_ecs_task_definition.pipeline_task_def.arn
  launch_type = "FARGATE"
  network_configuration {
    subnets = ["subnet-02a00c7be52b00368",
              "subnet-0d0b16e76e68cf51b",
              "subnet-081c7c419697dec52"]
    security_groups = ["sg-020697b6514174b72"]
    assign_public_ip = true
  }
  desired_count = 1  
}

# ECR for API

resource "aws_ecr_repository" "c9_deloton_api_t" {
  name                 = "c9-deloton-api-t"
  image_tag_mutability = "MUTABLE"
}

#Task definition for API

resource "aws_ecs_task_definition" "c9_deloton_api_task_def_t"{
  family                   = "c9-deloton-api-task-def-t"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn       = "${data.aws_iam_role.ecs_task_execution_role.arn}"
  container_definitions    = <<TASK_DEFINITION
[
  {
    "environment": [
      {"name": "DATABASE_IP", "value": "${var.DATABASE_IP}"},
      {"name": "DATABASE_NAME", "value": "${var.DATABASE_NAME}"},
      {"name": "DATABASE_PASSWORD", "value": "${var.DATABASE_PASSWORD}"},
      {"name": "DATABASE_PORT", "value": "${var.DATABASE_PORT}"},
      {"name": "DATABASE_USERNAME", "value": "${var.DATABASE_USERNAME}"}
    ],
    "name": "c9-ladybirds-load-old-data",
    "image": "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c9-ladybirds-load-old-data:latest",
    "essential": true
  }
]
TASK_DEFINITION

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }
}