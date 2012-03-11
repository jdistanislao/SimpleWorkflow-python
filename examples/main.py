from workflow_definitions import *

if __name__ == "__main__":

    wf_manager = define_wf()

    payload = WorkflowObject(payload='A', next_state='START')

    wf_manager.run_workflow(payload, get_wf())