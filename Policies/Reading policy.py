{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sqs:GetQueueAttributes",
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:SendMessage",
                "dynamodb:*"
            ],
            "Resource": [
                "arn:aws:sqs:eu-north-1:127865895568:Electricity_reading",
                "arn:aws:sqs:eu-north-1:127865895568:Gas_reading"
            ]
        }
    ]
}