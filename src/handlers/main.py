"""
 A Lambda function that returns a string.
"""


def lambda_handler(event, context):
    message = "Hello World"
    return {
        'message': message
    }
