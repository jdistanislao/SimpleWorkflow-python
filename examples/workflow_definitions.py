from workflow_extension import *

def define_wf():

    ###########################################################################
    # SYNC_1
    ###########################################################################
    sync_start_state = State('START')
    sync_a_state = State('A', ToBDirectLogicAdapter()) #ToBDirectLogicAdapter
    sync_b_state = State('B', ToEndDirectLogicAdapter())
    sync_end_state = State('END')

    sync_workflow = Workflow('sync')

    sync_workflow.register_state(sync_start_state)
    sync_workflow.register_state(sync_a_state)
    sync_workflow.register_state(sync_b_state)
    sync_workflow.register_state(sync_end_state)

    sync_workflow.add_transition(source_state='START', target_state='A', condition=PassThroughCondition())
    sync_workflow.add_transition(source_state='A', target_state='B', condition=BCondition())
    sync_workflow.add_transition(source_state='A', target_state='END', condition=EndCondition())
    sync_workflow.add_transition(source_state='B', target_state='END', condition=EndCondition())


    ###########################################################################
    # ASYNC
    ###########################################################################
    async_start_state = State('START')
    async_a_state = AsyncState('A', 'A-QUEUE', ToBDirectLogicAdapter()) #ToBDirectLogicAdapter
    async_b_state = AsyncState('B', 'B-QUEUE', ToEndDirectLogicAdapter())
    async_end_state = AsyncState('END', 'END-QUEUE')

    async_workflow = AsyncWorkflow('async')

    async_workflow.register_state(async_start_state)
    async_workflow.register_state(async_a_state)
    async_workflow.register_state(async_b_state)
    async_workflow.register_state(async_end_state)

    async_workflow.add_transition(source_state='START', target_state='A', condition=PassThroughCondition(), dispatcher=RabbitMQAsyncDispatcher())
    async_workflow.add_transition(source_state='A', target_state='B', condition=BCondition(), dispatcher=RabbitMQAsyncDispatcher())
    async_workflow.add_transition(source_state='A', target_state='END', condition=EndCondition(), dispatcher=RabbitMQAsyncDispatcher())
    async_workflow.add_transition(source_state='B', target_state='END', condition=EndCondition(), dispatcher=RabbitMQAsyncDispatcher())

    ###########################################################################
    # ASYNC_LOOP
    ###########################################################################
    async_loop_start_state = AsyncState('START', 'WF-QUEUE')
    async_loop_a_state = AsyncState('A', 'A-QUEUE', ToBDirectLogicAdapter()) #ToBDirectLogicAdapter
    async_loop_b_state = AsyncState('B', 'B-QUEUE', ToEndDirectLogicAdapter())
    async_loop_end_state = AsyncState('END', 'END-QUEUE')

    async_loop_workflow = AsyncLoopWorkflow('async_loop', 'WF-QUEUE', RabbitMQAsyncDispatcher())

    async_loop_workflow.register_state(async_loop_start_state)
    async_loop_workflow.register_state(async_loop_a_state)
    async_loop_workflow.register_state(async_loop_b_state)
    async_loop_workflow.register_state(async_loop_end_state)

    async_loop_workflow.add_transition(source_state='START', target_state='A', condition=PassThroughCondition(), dispatcher=RabbitMQAsyncDispatcher())
    async_loop_workflow.add_transition(source_state='A', target_state='B', condition=BCondition(), dispatcher=RabbitMQAsyncDispatcher())
    async_loop_workflow.add_transition(source_state='A', target_state='END', condition=EndCondition(), dispatcher=RabbitMQAsyncDispatcher())
    async_loop_workflow.add_transition(source_state='B', target_state='END', condition=EndCondition(), dispatcher=RabbitMQAsyncDispatcher())


    ###########################################################################
    # WF_MANAGER
    ###########################################################################
    wf_manager = WorkflowManager()
    wf_manager.add_wf(sync_workflow)
    wf_manager.add_wf(async_workflow)
    wf_manager.add_wf(async_loop_workflow)

    return  wf_manager

def get_wf():
    return 'sync' # async_loop async sync