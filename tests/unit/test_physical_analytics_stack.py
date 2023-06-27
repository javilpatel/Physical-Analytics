import aws_cdk as core
import aws_cdk.assertions as assertions

from physical_analytics.physical_analytics_stack import PhysicalAnalyticsStack

# example tests. To run these tests, uncomment this file along with the example
# resource in physical_analytics/physical_analytics_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = PhysicalAnalyticsStack(app, "physical-analytics")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
