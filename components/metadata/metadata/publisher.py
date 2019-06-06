from tensorflow.io import gfile
import os
import json
import hashlib
import fire

def publish(publisher_config, cached, execution, actual_outputs):
    
    base_dir = publisher_config['base_dir']
    if cached:
        outputs = execution['outputs']
    else:
        outputs = actual_outputs

    print('Outputs: ' + str(outputs))
    for o_key, o_value in outputs.items():
        output_path = os.path.join('/tmp', o_key)
        with open(output_path, 'w') as f:
            f.write(o_value)

    if not cached:
        execution['outputs'] = actual_outputs
        executions_dir = os.path.join(base_dir, 'executions')
        gfile.makedirs(executions_dir)
        execution_json = json.dumps(execution['inputs'], sort_keys = True)
        execution_id = hashlib.md5((execution_json).encode('utf-8')).hexdigest()
        execution_path = os.path.join(executions_dir, execution_id + '.json')
        print('Publish execution: ' + json.dumps(execution))
        with gfile.GFile(execution_path, 'w') as remote_file:
            json.dump(execution, remote_file)

if __name__ == '__main__':
    fire.Fire(publish)
