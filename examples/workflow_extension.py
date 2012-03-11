from workflow import *
import pika
import jsonpickle as jp


class ToBDirectLogicAdapter(LogicAdapter):

    def execute(self, wf_object=None):
        #print('Chiamo direttamente la logica con dati %s...' % payload.value)
        #print('La logica cambia il valore del payload da %s a B' % payload.value)
        wf_object.payload = 'B'
        #print('Cambio lo stato da %s a B' % payload.status)
        wf_object.headers[HDR_NEXT_STATE] = 'B'


class ToEndDirectLogicAdapter(LogicAdapter):

    def execute(self, wf_object=None):
        #print('Chiamo un direttamente logica con dati %s' % payload.value)
        #print('La logica cambia il valore del payload da %s a END' % payload.value)
        wf_object.payload = 'END'
        #print('Cambio lo stato da %s a END' % payload.status)
        wf_object.headers[HDR_NEXT_STATE] = 'END'


class StartState(State):

    def __init__(self, name=None):
        super(StartState, self).__init__()
        self.name = 'START'
        self.logic_adapter = LogicAdapter()


class EndState(State):

    def __init__(self, name=None):
        super(EndState, self).__init__()
        self.name = 'END'
        self.logic_adapter = LogicAdapter()

    def resolve_transition(self, wf_object=None):
        return EndDispatcher()


class BCondition(Condition):

    def evaluate(self, payload):
        return 'B' == payload.upper()


class EndCondition(Condition):

    def evaluate(self, payload):
        return 'END' == payload.upper()


class RabbitMQAsyncDispatcher(AsyncDispatcher):
    """
    Specifica implementazione dell'AsyncDispatcher per RabbitMQ.
    l'encoding del messaggio e' JSON
    """

    def send(self, wf_object, destination):
        parameters = pika.ConnectionParameters()
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=destination, durable=True, exclusive=False, auto_delete=False)
        channel.basic_publish(exchange='', routing_key=destination,
            body = jp.encode(wf_object),
            properties=pika.BasicProperties( content_type="text/plain", delivery_mode=1))
