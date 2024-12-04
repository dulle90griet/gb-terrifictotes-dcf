
provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      Team_name     = "Green Beans"
      Project_name  = "Green Beans Terrific Totes Solutions"
      Repo_name     = "gb-terrifictotes-solutions"
      Deployed_from = "Terraform"
      Environment   = "dev"
    }
  }
}

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~>5.0"
    }
  }

  backend "s3" {
    bucket = "gb-ttotes-remote-state-bucket"
    key    = "terraform.tfstate"
    region = "eu-west-2"
  }
}


data "aws_caller_identity" "current" {}

data "aws_region" "current" {}