from tensorflow.io import gfile
import os
import json
import hashlib
import fire

def init(driver_config, execution):
    base_dir = driver_config['base_dir']
    executions_dir = os.path.join(base_dir, 'executions')
    execution_json = json.dumps(execution['inputs'], sort_keys = True)
    execution_id = hashlib.md5((execution_json).encode('utf-8')).hexdigest()
    execution_path = os.path.join(executions_dir, execution_id + '.json')
    cached = False
    execution_data = json.dumps(execution)
    if gfile.exists(execution_path):
        cached = True
        with gfile.GFile(execution_path, 'r') as remote_file:
            execution_data = remote_file.read()

    print('Cached: ' + str(cached))
    with open('/tmp/cached', 'w') as f:
        f.write(str(cached))
    
    print('Execution: ' + execution_data)
    with open('/tmp/execution.json', 'w') as f:
        f.write(execution_data)

if __name__ == '__main__':
    fire.Fire(init)