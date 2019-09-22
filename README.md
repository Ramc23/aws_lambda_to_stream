# Overview

This module contains python scripts for a AWS lambda function


# What this module does

This lambda function, reads the events generated from the source (here, its is Kinesis data stream) and transfers the data to the remote destination (any server)

Lambda will be invoked via trigger (as soon as data is received by the source). Trigger here is Kinesis Data Stream.

By default, data transferred between AWS sdks are encrypted and Lambda provides serverless architecture.


# Lambda function creation

In AWS, select Lambda -> Functions -> Create function, In Create function - choose Python as Runtime.

Once Lambda function is created, choose upload a .zip file as Code entry type and upload this module