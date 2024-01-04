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
  username = var.database_username
  password = var.database_password
  skip_final_snapshot = true 
  availability_zone = "eu-west-2b"
  performance_insights_enabled = false

  db_subnet_group_name = "public_subnet_group"
  
  vpc_security_group_ids = [aws_security_group.c9_velo_securitygroup.id]
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
    image_uri = "129033205317.dkr.ecr.eu-west-2.amazonaws.com/c9-deloton-daily-report:latest"
    package_type  = "Image"
    environment {
      variables = {
      DATABASE_IP = "${var.database_ip}",
      DATABASE_NAME ="${var.database_name}",
      DATABASE_PASSWORD = "${var.database_password}",
      DATABASE_PORT ="${var.database_port}",
      DATABASE_USERNAME = "${var.database_username}",
      AWS_ACCESS = "${var.aws_access_key_id}",
      AWS_SECRET_ACCESS = "${var.aws_secret_access_key}"
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