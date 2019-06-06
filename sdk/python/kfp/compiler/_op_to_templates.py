# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import warnings
import yaml
from collections import OrderedDict
from typing import Union, List, Any, Callable, TypeVar, Dict

from ._k8s_helper import K8sHelper
from .. import dsl
from ..dsl._container_op import BaseOp
from ..dsl._artifact_location import ArtifactLocation

# generics
T = TypeVar('T')


def _process_obj(obj: Any, map_to_tmpl_var: dict):
    """Recursively sanitize and replace any PipelineParam (instances and serialized strings)
    in the object with the corresponding template variables
    (i.e. '{{inputs.parameters.<PipelineParam.full_name>}}').
    
    Args:
      obj: any obj that may have PipelineParam
      map_to_tmpl_var: a dict that maps an unsanitized pipeline
                       params signature into a template var
    """
    # serialized str might be unsanitized
    if isinstance(obj, str):
        # get signature
        param_tuples = dsl.match_serialized_pipelineparam(obj)
        if not param_tuples:
            return obj
        # replace all unsanitized signature with template var
        for param_tuple in param_tuples:
            obj = re.sub(param_tuple.pattern, map_to_tmpl_var[param_tuple.pattern], obj)

    # list
    if isinstance(obj, list):
        return [_process_obj(item, map_to_tmpl_var) for item in obj]

    # tuple
    if isinstance(obj, tuple):
        return tuple((_process_obj(item, map_to_tmpl_var) for item in obj))

    # dict
    if isinstance(obj, dict):
        return {
            key: _process_obj(value, map_to_tmpl_var)
            for key, value in obj.items()
        }

    # pipelineparam
    if isinstance(obj, dsl.PipelineParam):
        # if not found in unsanitized map, then likely to be sanitized
        return map_to_tmpl_var.get(
            str(obj), '{{inputs.parameters.%s}}' % obj.full_name)

    # k8s objects (generated from swaggercodegen)
    if hasattr(obj, 'swagger_types') and isinstance(obj.swagger_types, dict):
        # process everything inside recursively
        for key in obj.swagger_types.keys():
            setattr(obj, key, _process_obj(getattr(obj, key), map_to_tmpl_var))
        # return json representation of the k8s obj
        return K8sHelper.convert_k8s_obj_to_json(obj)

    # k8s objects (generated from openapi)
    if hasattr(obj, 'openapi_types') and isinstance(obj.openapi_types, dict):
        # process everything inside recursively
        for key in obj.openapi_types.keys():
            setattr(obj, key, _process_obj(getattr(obj, key), map_to_tmpl_var))
        # return json representation of the k8s obj
        return K8sHelper.convert_k8s_obj_to_json(obj)

    # do nothing
    return obj


def _process_base_ops(op: BaseOp):
    """Recursively go through the attrs listed in `attrs_with_pipelineparams`
    and sanitize and replace pipeline params with template var string.

    Returns a processed `BaseOp`.

    NOTE this is an in-place update to `BaseOp`'s attributes (i.e. the ones
    specified in `attrs_with_pipelineparams`, all `PipelineParam` are replaced
    with the corresponding template variable strings).

    Args:
        op {BaseOp}: class that inherits from BaseOp

    Returns:
        BaseOp
    """

    # map param's (unsanitized pattern or serialized str pattern) -> input param var str
    map_to_tmpl_var = {
        (param.pattern or str(param)): '{{inputs.parameters.%s}}' % param.full_name
        for param in op.inputs
    }

    # process all attr with pipelineParams except inputs and outputs parameters
    for key in op.attrs_with_pipelineparams:
        setattr(op, key, _process_obj(getattr(op, key), map_to_tmpl_var))

    return op


def _parameters_to_json(params: List[dsl.PipelineParam]):
    """Converts a list of PipelineParam into an argo `parameter` JSON obj."""
    _to_json = (lambda param: dict(name=param.full_name, value=param.value)
                if param.value else dict(name=param.full_name))
    params = [_to_json(param) for param in params]
    # Sort to make the results deterministic.
    params.sort(key=lambda x: x['name'])
    return params


# TODO: artifacts?
def _inputs_to_json(inputs_params: List[dsl.PipelineParam], _artifacts=None):
    """Converts a list of PipelineParam into an argo `inputs` JSON obj."""
    parameters = _parameters_to_json(inputs_params)
    return {'parameters': parameters} if parameters else None


def _outputs_to_json(op: BaseOp,
                     outputs: Dict[str, dsl.PipelineParam],
                     param_outputs: Dict[str, str],
                     output_artifacts: List[dict]):
    """Creates an argo `outputs` JSON obj."""
    if isinstance(op, dsl.ResourceOp):
        value_from_key = "jsonPath"
    else:
        value_from_key = "path"
    output_parameters = []
    for param in outputs.values():
        if param.name in param_outputs:
            value_path = param_outputs[param.name]
        elif param.value:
            value_path = '/tmp/kfp/outputs/%s' % param.name
        elif param.value_from:
            value_path = param.value_from
        else:
            value_path = None
        output_parameters.append({
            'name': param.full_name,
            'valueFrom': {
                value_from_key: value_path
            }
        })
    output_parameters.sort(key=lambda x: x['name'])
    ret = {}
    if output_parameters:
        ret['parameters'] = output_parameters
    if output_artifacts:
        ret['artifacts'] = output_artifacts

    return ret

def _op_to_templates(op: BaseOp):
    import copy
    op = _process_base_ops(op)
    exec_op = copy.deepcopy(op)
    exec_op.name = op.name + '-exec'
    return [_op_to_dag_template(op), _op_to_template(exec_op), _op_to_track_template(op)]

def _op_to_track_template(processed_op: BaseOp):
    import json
    template = {'name': processed_op.name + '-track'}
    # inputs = _inputs_to_json(processed_op.outputs.values())
    # if inputs:
    template['inputs'] = {
        'parameters': [{
            'name': 'execution'
        }]
    }
    if processed_op.outputs:
        param_outputs = { output.name: '/tmp/%s' % output.name for output in processed_op.outputs.values()}
        template['outputs'] = _outputs_to_json(processed_op, processed_op.outputs,
                                            param_outputs, [])
    # actual_outputs = { output.name : '{{inputs.parameters.%s}}' % output.full_name for output in processed_op.outputs.values()}
    template['container'] = {
        'image': 'gcr.io/hongyes-ml/metadata-tool:latest',
        'args': ['track.py', '{{inputs.parameters.execution}}']
    }
    return template

def _op_to_dag_template(processed_op: BaseOp):
    track_op_name = processed_op.name + '-track'
    exec_op_name = processed_op.name + '-exec'
    template = {'name': processed_op.name}
    inputs = _inputs_to_json(processed_op.inputs)
    if inputs:
        template['inputs'] = inputs

    exec_task = {
        'name': exec_op_name,
        'template': exec_op_name
    }

    if inputs:
        parameters = []
        for param in processed_op.inputs:
            parameters.append({
                'name': param.full_name,
                'value': '{{inputs.parameters.%s}}' % param.full_name
            })
        parameters.sort(key=lambda x: x['name'])
        exec_task['arguments'] = { 'parameters': parameters}
    

    import json
    execution = {
        'inputs': {param.full_name: '{{inputs.parameters.%s}}' % param.full_name for param in processed_op.inputs},
        'outputs': {param.name: '{{tasks.%s.outputs.parameters.%s}}' % (exec_op_name, param.full_name) for param in processed_op.outputs.values()}
    }
    track_task = {
        'name': track_op_name,
        'template': track_op_name,
        'dependencies': [exec_op_name],
        'arguments': {'parameters': [{
            'name': 'execution',
            'value': json.dumps(execution)
        }]}
    }
    
    if processed_op.outputs:
        output_parameters = []
        # track_input_parameters = []
        for param in processed_op.outputs.values():
            # track_input_parameters.append({
            #     'name': param.full_name,
            #     'value': '{{tasks.%s.outputs.parameters.%s}}' % (exec_op_name, param.full_name)
            # })
            output_parameters.append({
                'name': param.full_name,
                'valueFrom': {
                    'parameter': '{{tasks.%s.outputs.parameters.%s}}' % (track_op_name, param.full_name)
                }
            })
        # track_input_parameters.sort(key=lambda x: x['name'])
        # track_task['arguments'] = { 'parameters': track_input_parameters }

        output_parameters.sort(key=lambda x: x['name'])
        template['outputs'] = {
            'parameters': output_parameters
        }

    template['dag'] = {'tasks': [exec_task, track_task]}
    return template
    
# TODO: generate argo python classes from swagger and use convert_k8s_obj_to_json??
def _op_to_template(processed_op: BaseOp):
    """Generate template given an operator inherited from BaseOp."""

    # NOTE in-place update to BaseOp
    # replace all PipelineParams with template var strings
    # processed_op = _process_base_ops(op)

    if isinstance(processed_op, dsl.ContainerOp):
        # default output artifacts
        output_artifact_paths = OrderedDict(processed_op.output_artifact_paths)
        output_artifact_paths.setdefault('mlpipeline-ui-metadata', '/mlpipeline-ui-metadata.json')
        output_artifact_paths.setdefault('mlpipeline-metrics', '/mlpipeline-metrics.json')

        output_artifacts = [
             K8sHelper.convert_k8s_obj_to_json(
                 ArtifactLocation.create_artifact_for_s3(
                     processed_op.artifact_location, 
                     name=name, 
                     path=path, 
                     key='runs/{{workflow.uid}}/{{pod.name}}/' + name + '.tgz'))
            for name, path in output_artifact_paths.items()
        ]

        for output_artifact in output_artifacts:
            if output_artifact['name'] in ['mlpipeline-ui-metadata', 'mlpipeline-metrics']:
                output_artifact['optional'] = True

        input_artifacts = []
        for output in processed_op.outputs.values():
            if output.value:
                input_artifacts.append({
                    'name': output.name,
                    'path': '/tmp/kfp/outputs/%s' % output.name,
                    'raw': { 'data': output.value}
                })
        # workflow template
        template = {
            'name': processed_op.name,
            'container': K8sHelper.convert_k8s_obj_to_json(
                processed_op.container
            )
        }
    elif isinstance(processed_op, dsl.ResourceOp):
        # no output artifacts
        output_artifacts = []

        # workflow template
        processed_op.resource["manifest"] = yaml.dump(
            K8sHelper.convert_k8s_obj_to_json(processed_op.k8s_resource),
            default_flow_style=False
        )
        template = {
            'name': processed_op.name,
            'resource': K8sHelper.convert_k8s_obj_to_json(
                processed_op.resource
            )
        }

    # inputs
    inputs = _inputs_to_json(processed_op.inputs, input_artifacts)
    if inputs:
        template['inputs'] = inputs

    # outputs
    if isinstance(processed_op, dsl.ContainerOp):
        param_outputs = processed_op.file_outputs
    elif isinstance(processed_op, dsl.ResourceOp):
        param_outputs = processed_op.attribute_outputs
    template['outputs'] = _outputs_to_json(processed_op, processed_op.outputs,
                                           param_outputs, output_artifacts)

    # node selector
    if processed_op.node_selector:
        template['nodeSelector'] = processed_op.node_selector

    # tolerations
    if processed_op.tolerations:
        template['tolerations'] = processed_op.tolerations

    # metadata
    if processed_op.pod_annotations or processed_op.pod_labels:
        template['metadata'] = {}
        if processed_op.pod_annotations:
            template['metadata']['annotations'] = processed_op.pod_annotations
        if processed_op.pod_labels:
            template['metadata']['labels'] = processed_op.pod_labels
    # retries
    if processed_op.num_retries:
        template['retryStrategy'] = {'limit': processed_op.num_retries}

    # sidecars
    if processed_op.sidecars:
        template['sidecars'] = processed_op.sidecars

    return template
