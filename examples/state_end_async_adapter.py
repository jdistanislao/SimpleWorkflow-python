from pika.adapters import *
from workflow_definitions import *

connection = None
channel = None

###########################################################################
# WORKFLOW/QUEUE DEFINITIONS
###########################################################################
wf_manager = define_wf()
WF_NAME     = get_wf()
QUEUE_NAME = 'END-QUEUE'
###########################################################################
###########################################################################


def on_connected(connection):
    global channel
    connection.channel(on_channel_open)


def on_channel_open(channel_):
    global channel
    channel = channel_
    channel.queue_declare(queue=QUEUE_NAME, durable=True, exclusive=False, auto_delete=False, callback=on_queue_declared)


def on_queue_declared(frame):
    channel.basic_consume(handle_delivery, queue=QUEUE_NAME)


def handle_delivery(channel, method_frame, header_frame, body):
    print("Basic.Deliver %s delivery-tag %i: %s" % (header_frame.content_type, method_frame.delivery_tag, body))
    payload = jp.decode(body)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    execute_workflow(payload)


def execute_workflow(payload):
    wf_manager.run_workflow(payload, WF_NAME)


if __name__ == '__main__':
    parameters = pika.ConnectionParameters()
    connection = SelectConnection(parameters, on_connected)
    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
        connection.ioloop.start()
