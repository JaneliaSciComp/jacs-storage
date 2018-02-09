import json
import requests
import sys


AUTH_URL = 'https://jacs-dev.int.janelia.org//SCSW/AuthenticationService/v1/authenticate'
STORAGE_URL = 'http://localhost:8880/jacsstorage/master_api/v1'


def auth(username, password):
    response = requests.post(AUTH_URL, data={
        'username': username,
        'password': password
    })
    if response.status_code != 200:
        raise Exception("Authentication failed " + response.text)

    return response.json()['token']


def allocate_storage(username, storagename, token):
    ownerkey = 'user:' + username
    response = requests.post(STORAGE_URL + '/storage',
                             json = {
                                 'ownerKey': ownerkey,
                                 'name': storagename,
                                 'storageFormat': 'DATA_DIRECTORY'
                             },
                             headers = {
                                 'Authorization': 'Bearer ' + token
                             })
    if response.status_code != 201:
        raise Exception("Storage allocation failed " + response.text)

    return response.json()


def add_dir(storage_url, storage_id, dir_relative_path, token):
    '''
    Adds a storage directory. This method also adds all parent directories. If the directory or any of the parent
    directories already exists the method will fail.
    :param storage_url:
    :param storage_id:
    :param dir_relative_path:
    :param token:
    :return:
    '''
    subdirs = dir_relative_path.split('/')
    subpath = ''
    for dn in subdirs:
        subpath = subpath + '/' + dn
        response = requests.post(storage_url + '/agent_storage/' + storage_id + '/directory' + subpath,
                             headers = {
                                 'Authorization': 'Bearer ' + token
                             })
    if response.status_code != 201:
        raise Exception("Storage allocation failed " + response.text)

    return response.json()


def add_file(storage_url, storage_id, file_relative_path, local_file_name, token):
    '''
    Add a file and create all its parent directories
    :param storage_url:
    :param storage_id:
    :param file_relative_path:
    :param local_file_name:
    :param token:
    :return:
    '''
    pathcomponents = file_relative_path.split('/')
    if len(pathcomponents) > 1:
        add_dir(storage_url, storage_id, '/'.join(pathcomponents[:-1]), token)

    with open(local_file_name, "rb") as lfh:
        local_content = lfh.read()
        response = requests.post(storage_url + '/agent_storage/' + storage_id + '/file/' + file_relative_path,
                                 headers={
                                     'Authorization': 'Bearer ' + token
                                 },
                                 data = local_content)
        if response.status_code != 201:
            raise Exception("Storage allocation failed " + response.text)

        return response.json()


def list_storage(storage_url, storage_id, storage_entry_name, token):
    response = requests.get(storage_url + '/agent_storage/' + storage_id + '/list',
                            headers={
                                 'Authorization': 'Bearer ' + token
                            },
                            params={
                                'entry': storage_entry_name
                            })
    if response.status_code != 200:
        raise Exception("Storage allocation failed " + response.text)

    return response.json()


def get_content_bytes(storage_url, storage_id, storage_entry_name, token):
    response = requests.get(storage_url + '/agent_storage/' + storage_id + '/entry_content/' + storage_entry_name,
                            headers={
                                'Authorization': 'Bearer ' + token
                            })
    if response.status_code != 200:
        raise Exception("Storage allocation failed " + response.text)

    return response.content


def main():
    username = sys.argv[1]
    password = sys.argv[2]
    storagename = sys.argv[3]
    storage_file_path = sys.argv[4]
    local_file_name = sys.argv[5]

    jwt = auth(username, password)
    print(jwt)
    storage = allocate_storage(username, storagename, jwt)
    print(storage)
    storage_file_info = add_file(storage['connectionURL'], storage['id'], storage_file_path, local_file_name, jwt)
    print(storage_file_info)
    all_storage_entries = list_storage(storage['connectionURL'], storage['id'], '', jwt)
    print(all_storage_entries)
    storage_entry = list_storage(storage['connectionURL'], storage['id'], storage_file_path, jwt)
    print(storage_entry)
    storage_entry_content = get_content_bytes(storage['connectionURL'], storage['id'], storage_file_path, jwt)
    print('Read', len(storage_entry_content), 'bytes')


if __name__ == '__main__':
        sys.exit(main())