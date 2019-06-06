from ..dsl._ops_group import OpsGroup, Graph, Condition
from .. import dsl
from ..dsl import _container_op
from ..dsl._container_op import ContainerOp
from ._k8s_helper import K8sHelper
import json
import copy
from .. import gcp

def _insert_metadata_groups(group, pipeline):
    config = {
        'base_dir': 'gs://hongyes-ml-tests/metadata'
    }

    def register_op_and_generate_id(op):
      return pipeline.add_op(op, define_only = True)

    _old__register_op_handler = _container_op._register_op_handler
    _container_op._register_op_handler = register_op_and_generate_id
    ops = group.ops.copy()
    groups = group.groups.copy()
    for op in ops:
        op_name = K8sHelper.sanitize_k8s_name(op.name)
        group.ops.remove(op)
        del pipeline.ops[op_name]
        dag = Graph(op_name + '-group')
        inputs = {
            'spec': json.dumps({
                'image': op.image,
                'command': op.command
            })
        }
        for input in op.inputs:
            inputs[input.name] = str(input)
        execution = {'inputs': inputs }
        print(execution)
        driver_op = ContainerOp(
            name=op.name + '-driver',
            image='gcr.io/hongyes-ml/metadata-tool:latest',
            arguments=['driver.py', json.dumps(config), json.dumps(execution)],
            file_outputs={
                'cached': '/tmp/cached',
                'execution': '/tmp/execution.json'
            }
        ).apply(gcp.use_gcp_secret('gcp-user', '/key.json'))
        condition_dag = Condition(driver_op.outputs['cached'] == 'False')
        condition_dag.name = op_name + '-condition'
        executor_op = copy.deepcopy(op)
        # print(executor_op.name)
        executor_op.name = op_name + '-executor'
        pipeline.ops[executor_op.name] = executor_op
        for _, output in executor_op.outputs.items():
            output.op_name = executor_op.name
        condition_dag.ops.append(executor_op)
        dag.ops.append(driver_op)
        dag.groups.append(condition_dag)
        actual_outputs = { output.name : str(output) for output in executor_op.outputs.values()}
        file_outputs = { key: '/tmp/{}'.format(output.name) for key, output in executor_op.outputs.items()}
        publisher_op = ContainerOp(
            name=op.name + '-publisher',
            image='gcr.io/hongyes-ml/metadata-tool:latest',
            arguments=['publisher.py', json.dumps(config), driver_op.outputs['cached'], 
                driver_op.outputs['execution'], json.dumps(actual_outputs)],
            file_outputs=file_outputs
        ).apply(gcp.use_gcp_secret('gcp-user', '/key.json'))
        dag.ops.append(publisher_op)
        for op in pipeline.ops.values():
            for input in op.inputs:
                if input.op_name == op_name:
                    input.op_name = publisher_op.name
            if op_name in op.dependent_names:
                op.dependent_names.remove(op_name)
                op.dependent_names.append(publisher_op.name)
        _replace_condition_param_op_name(group, op_name, publisher_op.name)
        # publisher_op.after(condition_dag)
        # op.name = op.name + '-publisher'
        group.groups.append(dag)

    _container_op._register_op_handler = _old__register_op_handler

    for group in groups:
        _insert_metadata_groups(group, pipeline)

def _replace_condition_param_op_name(group, old_op_name, new_op_name):
    if group.type == 'condition':
        condition_params = []
        if isinstance(group.condition.operand1, dsl.PipelineParam):
            condition_params.append(group.condition.operand1)
        if isinstance(group.condition.operand2, dsl.PipelineParam):
            condition_params.append(group.condition.operand2)

        for param in condition_params:
            if param.op_name == old_op_name:
                param.op_name = new_op_name

    
    for subgroup in group.groups:
        _replace_condition_param_op_name(subgroup, old_op_name, new_op_name)