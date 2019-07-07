'''Docker utilities'''

import os
import sys
import json
import logging
from os import path


def get_logger(name, stdout=False):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    filename = f'{name.replace(":","__").replace("/","-")}.log'
    file_handler = logging.FileHandler(filename=filename)
    logger.addHandler(file_handler)
    if stdout:
        stdout_handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(stdout_handler)
    
    return logger

def get_dockerfile():
    '''return path to dockerfile

    The following locations are scanned in order
    for the Dockerfile
    1. ~/.ae5/Dockerfile
    2. <site-packages>/ae5_tools/Dockerfile.dist
    '''

    _path = path.expanduser(os.getenv('AE5_TOOLS_CONFIG_DIR') or '~/.ae5')
    user_file = path.join(_path, 'Dockerfile')
    dist_file = path.join(path.dirname(__file__), 'Dockerfile.dist')
    if path.exists(user_file):
        dockerfile = user_file
    elif path.exists(dist_file):
        dockerfile = dist_file
    else:
        msg = f'''Dockerfile was not found in any of the following locations
{user_file}
{dist_file}
Please check that the file exists or that ae5-tools was installed properly.
Otherwise, please file a bug report.'''
        raise FileNotFoundError(msg)

    with open(dockerfile) as f:
        contents = f.read()
    return contents


def build_image(path, tag, ae5_hostname=None, debug=False):
    try:
        import docker
    except ModuleNotFoundError as e:
        print('You must install docker-py to build an image')
        raise(e)
    
    client = docker.from_env()

    buildargs={'CHANNEL_ALIAS':f'https://{ae5_hostname}/repository/conda/'} if ae5_hostname else None
    logger = get_logger(tag, stdout=debug)
    print(f'Build logs written to {logger.handlers[0].baseFilename}')
    try:
        logger.debug(f'*** {tag} image build starting')
        for line in client.api.build(path=path, tag=tag, buildargs=buildargs):
            text = line.decode().strip()
            logger.info(text)
            try:
                d = json.loads(text)
                error = d.get('error', False)
                if error:
                    raise docker.errors.BuildError(d['error'], line)
            except json.decoder.JSONDecodeError:
                pass


    except docker.errors.BuildError as e:
        print('An error was encountered while building the image.')
        print(e)
    
    return tag
