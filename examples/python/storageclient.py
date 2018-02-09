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

def add_dir(storage_url, storage_id, dirpath, token):
    subdirs = dirpath.split('/')
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

def main():
    username = sys.argv[1]
    password = sys.argv[2]
    storagename = sys.argv[3]
    dirpath = sys.argv[4]

    jwt = auth(username, password)
    print(jwt)
    storage = allocate_storage(username, storagename, jwt)
    print(storage)
    storagedir = add_dir(storage['connectionURL'], storage['id'], dirpath, jwt)
    print(storagedir)


if __name__ == '__main__':
        sys.exit(main())