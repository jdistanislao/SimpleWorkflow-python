__author__ = 'Jacopo Di Stanislao'

# Header's keys
HDR_NEXT_STATE = 'NEXT_STATE'
# others headers here...


class WorkflowObject(object):
    """
    Oggetto che transita tra i vari stati del workflow
    """

    payload = None  # payload del workflow
    headers = None  # Headers utili al workflow

    def __init__(self, payload=None, next_state=None):
        self.payload = payload
        self.headers = { HDR_NEXT_STATE: next_state }


class LogicAdapter(object):
    """
    Base LogicAdapter con logica assente
    """

    def execute(self, wf_object=None):
        """
        Esegue la logica
        """
        pass


class State(object):
    """
    Base State
    Se non si specifica un logic adapter imposta come default LogicAdapter (logica assente)
    """

    def __init__(self, name=None, logic_adapter=None):
        self.name = name
        self.transitions = []
        self.logic_adapter = LogicAdapter() if not logic_adapter else logic_adapter

    def execute(self, wf_object=None):
        """
        Esegue la logica associata allo State
        """
        self.logic_adapter.execute(wf_object)

    def add_transition(self, transition=None):
        """
        Aggiunge una transizione allo State
        """
        if transition:
            self.transitions.append(transition)

    def resolve_transition(self, wf_object=None):
        """
        Risolve una transizione a seguito dell'invocazione al metodo execute() e ritorna il dispatcher
        associato ad essa.
        Imposta come dispatcher di default EndDispatcher cosi' da non far proseguire il workflow
        nel caso di una transizione non trovata
        """
        dispatcher = EndDispatcher()
        for transition in self.transitions:
            if transition.verify(wf_object):
                wf_object.headers[HDR_NEXT_STATE] = transition.destination
                dispatcher = transition.dispatcher
                break
        return dispatcher


class AsyncState(State):
    """
    State asincrono.
    Aggiunge il parametro route per poter instradare il messaggio. Se route non e' impostato, di default
    ne crea uno utilizzando il nome della stato aggiungendo ROUTE_SUFFIX
    """

    ROUTE_SUFFIX = '-QUEUE'

    def __init__(self, name=None, route=None, logic_adapter=None):
        super(AsyncState, self).__init__(name, logic_adapter)
        self.create_route(route)

    def create_route(self, route=None):
        """
        Definisce il nome del campo route
        """
        self.route = route if route else self.name+self.ROUTE_SUFFIX


class Transition(object):
    """
    Base Transition
    """

    def __init__(self, destination=None, condition=None, dispatcher=None):
        self.destination = destination
        self.condition = condition
        self.dispatcher = dispatcher

    def verify(self, wf_object=None):
        """
        Verifica se la condizione della transizione e' verificata o meno
        """
        if self.condition and not self.condition.verify(wf_object):
            return False
        else:
            wf_object.headers[HDR_NEXT_STATE] = self.destination
            return True


class SyncTransition(Transition):
    """
    Transizione sincrona.
    Imposta di default un SyncDispatcher indipendentemente da quello passato per permettere di continuare
    il ciclo del workflow
    """

    def __init__(self, destination=None, condition=None, dispatcher=None):
        super(SyncTransition, self).__init__(destination, condition, dispatcher)
        self.dispatcher = SyncDispatcher(destination)


class AsyncTransition(Transition):
    """
    Transizione asincrona.
    Introduce il parametro route per identificare dove verra' spedito il messaggio.
    """

    def __init__(self, destination=None, route=None, condition=None, dispatcher=None):
        super(AsyncTransition, self).__init__(destination, condition, dispatcher)
        self.route = route
        dispatcher.destination = self.route


class Condition(object):
    """
    Base Condition
    """

    def verify(self, wf_object=None):
        """
        Verifica che sia presente il wf_object e che la condizione sia corretta
        """
        return True if wf_object and self.evaluate(wf_object.payload) else False

    def evaluate(self, payload):
        """
        Logica per valutare se la condizione e' corretta o meno
        """
        pass


class PassThroughCondition(Condition):
    """
    Condizione che viene sempre verificata
    """

    def evaluate(self, wf_object):
        return True


class Dispatcher(object):
    """
    Base dispatcher
    """

    def __init__(self, destination=None):
        self.destination = destination

    def dispatch(self, wf_object):
        """
        Esegue il dispatch e torna True e False per far continuare o meno il workflow.
        """
        pass


class SyncDispatcher(Dispatcher):
    """
    Dispatcher sincrono che permette di continuare il ciclo di computazione del workflow.
    Non esegue nessuna operazione.
    """

    def dispatch(self, wf_object):
        return True


class AsyncDispatcher(Dispatcher):
    """
    Dispatcher asincrono che termina il ciclo di computazione del workflow.
    Esegue una send() per spedire il messaggio verso un consumer asincrono.
    """

    def dispatch(self, wf_object):
        self.send(wf_object, self.destination)
        return False

    def send(self, wf_object, destination):
        """
        Utilizza il client dedicato per spedire il messaggio
        """
        pass


class EndDispatcher(Dispatcher):
    """
    Dispatcher sincrono che termina il ciclo di computazione del workflow.
    Si comporta come un dispatcher asincrono ma non utilizza un client
    """

    def dispatch(self, wf_object):
        return False


class Workflow(object):
    """
    Base Workflow sincrono
    """

    def __init__(self, name=None):
        self.name = name
        self.state_map = {}

    def register_state(self, state=None):
        """
        Aggiunge uno State al workflow
        """
        if state:
            self.state_map[state.name] = state

    def add_transition(self, source_state=None, target_state=None, condition=None, dispatcher=None):
        """
        Aggiunge una transizione tra due stati
        """
        if self.state_map.has_key(source_state) and self.state_map.has_key(target_state) and condition:
            source = self.state_map.get(source_state)
            target = self.state_map.get(target_state)
            transition = self.create_transition(target_state=target, condition=condition, dispatcher=dispatcher)
            source.add_transition(transition)

    def create_transition(self, target_state=None, condition=None, dispatcher=None):
        """
        Crea una transizione sincrona tra due stati
        """
        return SyncTransition(destination=target_state.name, condition=condition, dispatcher=dispatcher)

    def resolve_next_state(self, wf_object=None):
        """
        Ritorna il prossimo stato da eseguire
        """
        if wf_object and self.state_map.has_key(wf_object.headers.get(HDR_NEXT_STATE)):
            return self.state_map.get(wf_object.headers.get(HDR_NEXT_STATE))
        else:
            return None


class AsyncWorkflow(Workflow):
    """
    Workflow asincrono dove ogni stato e' dietro a code
    """

    def create_transition(self, target_state=None, condition=None, dispatcher=None):
        """
        Crea una transizione asincrona tra due stati
        """
        return AsyncTransition(destination=target_state.name, route=target_state.route,
            condition=condition, dispatcher=dispatcher)


class AsyncLoopWorkflow(Workflow):
    """
    Workflow asincrono con coda di workflow.
    In questo caso gli stati dopo aver eseguito al logica ricoodano il messaggio sulla code del workflow
    che poi si occupera' di effettuare il dispatch verso la coda opportuna
    """

    def __init__(self, name=None, loop_route=None, dispatcher=None):
        super(AsyncLoopWorkflow, self).__init__(name)
        Workflow.__init__(self, name)
        self.loop_route = loop_route
        self.dispatcher = dispatcher
        self.destinations_route_map = {}

    def create_transition(self, target_state=None, condition=None, dispatcher=None):
        """
        Crea una transizione asincrona tra due stati modificando la route per farla puntare alla destinazione
        del workflow
        """
        self.destinations_route_map[target_state.name] = target_state.route
        return AsyncTransition(destination=target_state.name, route=self.loop_route,
            condition=condition, dispatcher=dispatcher)


class WorkflowManager(object):
    """
    Manager del workflow.
    """

    def __init__(self):
        self.wf_map = {}

    def add_wf(self, workflow=None):
        """
        Aggiunge un workflow al manager
        """
        if workflow:
            self.wf_map[workflow.name] = workflow

    def run_workflow(self, wf_object=None, workflow=None):
        """
        Definisce la sequenza di azioni da richiamare ad ogni iterazione
        per un workflow sincrono e asincrono
        """
        if wf_object and workflow and self.wf_map.has_key(workflow):
            wf = self.wf_map.get(workflow)
            sync_iterate = True
            while sync_iterate and wf.resolve_next_state(wf_object):
                state = wf.resolve_next_state(wf_object)
                #print('Executing %s' % state.name)
                state.execute(wf_object)
                dispatcher = state.resolve_transition(wf_object)
                sync_iterate = dispatcher.dispatch(wf_object)

    def run_async_loop_workflow(self, wf_object=None, workflow=None):
        """
        Definisce la sequenza di azioni da richiamare ad ogni iterazione
        per un workflow di tipo AsyncLoopWorkflow
        """
        if wf_object and workflow and self.wf_map.has_key(workflow):
            wf = self.wf_map.get(workflow)
            if isinstance(wf, AsyncLoopWorkflow) and wf.resolve_next_state(wf_object):
                state = wf.resolve_next_state(wf_object)
                #print('Dispatching to %s' % state.name)
                wf.dispatcher.destination = wf.destinations_route_map.get(state.name)
                wf.dispatcher.dispatch(wf_object)