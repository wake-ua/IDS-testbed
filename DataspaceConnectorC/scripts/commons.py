#!/usr/bin/env python
import requests
from requests.exceptions import HTTPError
import csv

# constants
LANGS = ["es", "ca", "en"]


def ckan_api_request(ckan_url: str, endpoint: str, method: str, data: dict = {},
                     params: dict = {}, files: list = [],
                     token: str = None,
                     content: str = 'application/json',
                     verbose=True) -> (int, dict):

    api_url = "{}/api/3/action/".format(ckan_url)

    # set headers
    headers = {}
    if token:
        headers['Authorization'] = token
    if content:
        headers['Content-Type'] = content

    # do the actual call
    try:
        if method == 'post':
            response = requests.post('{}{}'.format(api_url, endpoint), json=data, params=params,
                                     files=files, headers=headers)
        else:
            response = requests.get('{}{}'.format(api_url, endpoint), params=params, headers=headers)

        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        result = response.json()
        return 0, result

    except HTTPError as http_err:
        if verbose:
            print(f'\t HTTP error occurred: {http_err} {response.json()}')  # Python 3.6
        result = {"http_error": http_err, "error": response.json()}
    except Exception as err:
        if verbose:
            print(f'\t Other error occurred: {err}')  # Python 3.6
        result = {"error": err}

    return -1, result


def read_groups(file_path: str) -> list:
    # read the groups file
    print(" - Read input file: {}".format(file_path))

    groups = []

    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            groups += [row]
            # print("\t * {}".format(row))

    print(" \t -> Read {} groups(s): {}".format(len(groups), ', '.join([group['name'] for group in groups])))

    return groups


def read_vocabulary(file_path: str) -> list:
    # read the tags file
    print(" - Read input file: {}".format(file_path))

    tags = []

    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')

        for row in reader:
            new_row = {}

            for field, value in row.items():
                if field.rsplit('_', 1)[-1] in LANGS:
                    parent_field = field.rsplit('_', 1)[0].strip()
                    lang = field.rsplit('_', 1)[-1]
                    translated_field = new_row.get(parent_field, {})
                    translated_field[lang] = value
                    new_row[parent_field] = translated_field
                else:
                    new_row[field] = value

            tags += [new_row]

    return tags
