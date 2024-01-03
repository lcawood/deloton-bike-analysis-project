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