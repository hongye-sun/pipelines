import os
import fire

def track(execution):
    print('Execution: ' + str(execution))
    for o_key, o_value in execution['outputs'].items():
        output_path = os.path.join('/tmp', o_key)
        with open(output_path, 'w') as f:
            f.write(o_value)

if __name__ == '__main__':
    fire.Fire(track)
