import os
import json
import requests
import commons
import lxml.html
from dotenv import load_dotenv

load_dotenv('.env')

METADATA_BROKER_URL = os.getenv("METADATA_BROKER_URL")
METADATA_BROKER_DOCKER_URL = os.getenv("METADATA_BROKER_DOCKER_URL")
CONNECTOR_URL = os.getenv("CONNECTOR_URL")
CONNECTOR_DOCKER_URL = os.getenv("CONNECTOR_DOCKER_URL")
CONNECTOR_USER = os.getenv('CONNECTOR_USER')
CONNECTOR_PW = os.getenv('CONNECTOR_PW')
DATA_SOURCE_URL = os.getenv('DATA_SOURCE_URL')
DATASET_LIST = os.getenv('DATASET_LIST')
RULE_JSON = os.getenv('RULE_JSON')


def get_dataset_list(input_file: str = DATASET_LIST):
    datasets = []
    with open(input_file, 'r') as source:
        for line in source.readlines():
            if len(line.strip()) > 3 and not line.startswith('#'):
                dataset = line.strip()
                if dataset.startswith('http'):
                    dataset = dataset.split('/')[-1]
                datasets += [dataset]
    return datasets


def get_rule(input_file: str = RULE_JSON):
    with open(input_file, 'r') as source:
        rule = source.read()
    return rule


# Get broker description
def get_broker_description(metadata_broker_url: str) -> dict:
    response = requests.get(metadata_broker_url, verify=False)
    print(" \t * Request GET {0} \t => {1}".format(metadata_broker_url, response.status_code))
    content = response.content
    description = json.loads(content)

    return description


def get_self_description(connector_url: str, auth: tuple) -> list:

    request_url = "{0}".format(connector_url)
    response = requests.get(request_url, data={}, auth=auth, verify=False)
    print(" \t - Request GET {0} \t => {1}".format(request_url, response.status_code))
    description = json.loads(response.content)

    return description


def get_provider_catalog_description(provider_docs: list, connector_url: str, auth: tuple) -> (list, list):
    catalogs = []
    resources = []

    for provider in provider_docs:
        provider_url = provider["_provider_url"]
        for provider_catalog in provider["_catalogs"]:
            provider_catalog_id = provider_catalog['@id']
            request_url = "{0}/api/ids/description?recipient={1}&elementId={2}".format(connector_url, provider_url,
                                                                                       provider_catalog_id)
            response = requests.post(request_url, data={}, auth=auth, verify=False)
            print(" \t - Request POST {0} \t => {1}".format(request_url, response.status_code))
            content = response.content
            catalog = json.loads(content)

            catalog["_provider_id"] = provider['@id']
            for k in ["_broker_id", "_broker_catalog_id", "_broker_connector_id", "_provider_url"]:
                catalog[k] = provider[k]

            catalog_resources = catalog["ids:offeredResource"]
            catalog["ids:offeredResource"] = [str(r['@id']) for r in catalog_resources]
            catalogs += [catalog]

            for resource in catalog_resources:
                for k in ["_broker_id", "_broker_catalog_id", "_broker_connector_id", "_provider_url", "_provider_id"]:
                    resource[k] = catalog[k]
                resource["_catalog_id"] = str(catalog['@id'])
                resources += [resource]
    return catalogs, resources


def upsert_catalog(catalog_data: dict, connector_url: str, auth: tuple) -> dict:
    catalog_org_id = catalog_data["organization_id"]

    # check if catalog exists
    request_url = "{0}/api/catalogs".format(connector_url)
    response = requests.get(request_url, data={}, auth=auth, verify=False)
    print(" \t\t\t\t - Request GET Catalogs {0} \t => {1}".format(request_url, response.status_code))
    catalog_list = json.loads(response.content).get('_embedded', {}).get('catalogs', [])

    existing_catalogs = [c for c in catalog_list if c.get("additional", {}).get("organization_id") == catalog_org_id]

    if len(existing_catalogs) == 0:
        # POST
        request_url = "{0}/api/catalogs".format(connector_url)
        response = requests.post(request_url, json=catalog_data, auth=auth, verify=False)
        print(" \t\t\t\t - Request POST new Catalog {0} \t => {1}".format(request_url, response.status_code))
        new_catalog = json.loads(response.content)
        return new_catalog
    elif len(existing_catalogs) == 1:
        # PUT
        request_url = existing_catalogs[0]["_links"]["self"]["href"]
        response = requests.put(request_url, json=catalog_data, auth=auth, verify=False)
        print(" \t\t\t\t - Request PUT updated Catalog {0} \t => {1}".format(request_url, response.status_code))
        if response.status_code == 204:
            response = requests.get(request_url, data={}, auth=auth, verify=False)
            print(" \t\t\t\t - Request GET Catalog {0} \t => {1}".format(request_url, response.status_code))
            updated_catalog = json.loads(response.content)
            return updated_catalog
        else:
            raise("*ERROR* Could not update catalog {}: {}".format(catalog_org_id, response))
    else:
        raise Exception("*ERROR: Multiple catalogs matching current organization {}: {}".format(catalog_org_id,
                                                                                                existing_catalogs))


def upsert_offer(offer_data: dict, connector_url: str, auth: tuple) -> dict:
    resource_id = offer_data['resource_id']

    # check if offer exists
    request_url = "{0}/api/offers".format(connector_url)
    response = requests.get(request_url, data={}, auth=auth, verify=False)
    print(" \t\t\t\t - Request GET offers {0} \t => {1}".format(request_url, response.status_code))
    response.raise_for_status()
    offer_list = json.loads(response.content).get('_embedded', {}).get('resources', [])
    existing_offers = [o for o in offer_list if o.get("additional", {}).get("resource_id") == resource_id]

    if len(existing_offers) == 0:
        # POST
        request_url = "{0}/api/offers".format(connector_url)
        response = requests.post(request_url, json=offer_data, auth=auth, verify=False)
        print(" \t\t\t\t - Request POST new Offer {0} \t => {1}".format(request_url, response.status_code))
        response.raise_for_status()
        new_offer = json.loads(response.content)
        return new_offer
    elif len(existing_offers) == 1:
        # PUT
        request_url = existing_offers[0]["_links"]["self"]["href"]
        response = requests.put(request_url, json=offer_data, auth=auth, verify=False)
        print(" \t\t\t\t - Request PUT updated Offer {0} \t => {1}".format(request_url, response.status_code))
        response.raise_for_status()
        if response.status_code == 204:
            response = requests.get(request_url, data={}, auth=auth, verify=False)
            print(" \t\t\t\t - Request GET Offer {0} \t => {1}".format(request_url, response.status_code))
            response.raise_for_status()
            updated_offer = json.loads(response.content)
            return updated_offer
        else:
            raise("*ERROR* Could not update offer {}: {}".format(resource_id, response))
    else:
        raise Exception("*ERROR: Multiple offers matching current organization {}: {}".format(resource_id,
                                                                                              existing_offers))


def upsert_resource_entity(entity_data: dict, entity_name: str, connector_url: str, auth: tuple) -> dict:
    resource_id = entity_data['resource_id']

    # check if entity exists
    request_url = "{0}/api/{1}".format(connector_url, entity_name)
    response = requests.get(request_url, data={}, auth=auth, verify=False)
    print(" \t\t\t\t - Request GET {0} {1}\t => {2}".format(entity_name, request_url, response.status_code))
    response.raise_for_status()
    entity_list = json.loads(response.content).get('_embedded', {}).get(entity_name, [])
    existing_entities = [o for o in entity_list if o.get("additional", {}).get("resource_id") == resource_id]

    if len(existing_entities) == 0:
        # POST
        response = requests.post(request_url, json=entity_data, auth=auth, verify=False)
        print(" \t\t\t\t - Request POST new {0} {1} \t => {2}".format(entity_name, request_url, response.status_code))
        response.raise_for_status()
        new_entity = json.loads(response.content)
        return new_entity
    elif len(existing_entities) == 1:
        # PUT
        request_url = existing_entities[0]["_links"]["self"]["href"]
        response = requests.put(request_url, json=entity_data, auth=auth, verify=False)
        print(" \t\t\t\t - Request PUT updated entity {0} {1}\t => {2}".format(entity_name, request_url, response.status_code))
        response.raise_for_status()
        if response.status_code == 204:
            response = requests.get(request_url, data={}, auth=auth, verify=False)
            print(" \t\t\t\t - Request GET entity {0} \t => {1}".format(request_url, response.status_code))
            response.raise_for_status()
            updated_entity = json.loads(response.content)
            return updated_entity
        else:
            raise("*ERROR* Could not update entity {}: {}".format(resource_id, response))
    else:
        raise Exception("*ERROR: Multiple entities matching current organization {}: {}".format(
            resource_id, existing_entities))


def add_artifact_to_representation(artifact: dict, representation: dict, auth: tuple) -> dict:
    representation_url = representation["_links"]["self"]["href"]
    artifact_url = artifact["_links"]["self"]["href"]
    request_url = "{}/artifacts".format(representation_url)
    response = requests.post(request_url, json=[artifact_url], auth=auth, verify=False)
    print(" \t\t\t\t - Request POST add artifact to representation {0} \t => {1}".format(request_url,
                                                                                      response.status_code))
    response.raise_for_status()


def add_offer_to_catalog(offer: dict, catalog: dict, auth: tuple) -> dict:
    catalog_url = catalog["_links"]["self"]["href"]
    offer_url = offer["_links"]["self"]["href"]
    request_url = "{}/offers".format(catalog_url)
    response = requests.post(request_url, json=[offer_url], auth=auth, verify=False)
    print(" \t\t\t\t - Request POST add offer to catalog {0} \t => {1}".format(request_url, response.status_code))
    response.raise_for_status()


def add_representation_to_offer(representation: dict, offer: dict, auth: tuple) -> dict:
    offer_url = offer["_links"]["self"]["href"]
    representation_url = representation["_links"]["self"]["href"]
    request_url = "{}/representations".format(offer_url)
    response = requests.post(request_url, json=[representation_url], auth=auth, verify=False)
    print(" \t\t\t\t - Request POST add artifact to representation {0} \t => {1}".format(request_url,
                                                                                      response.status_code))
    response.raise_for_status()


def add_rule_to_contract(rule: dict, contract: dict, auth: tuple) -> dict:
    contract_url = contract["_links"]["self"]["href"]
    rule_url = rule["_links"]["self"]["href"]
    request_url = "{}/rules".format(contract_url)
    response = requests.post(request_url, json=[rule_url], auth=auth, verify=False)
    print(" \t\t\t\t - Request POST add rule to contract {0} \t => {1}".format(request_url, response.status_code))
    response.raise_for_status()


def add_contract_to_offer(contract: dict, offer: dict, auth: tuple) -> dict:
    contract_url = contract["_links"]["self"]["href"]
    offer_url = offer["_links"]["self"]["href"]
    request_url = "{}/contracts".format(offer_url)
    response = requests.post(request_url, json=[contract_url], auth=auth, verify=False)
    print(" \t\t\t\t - Request POST add contract to offer {0} \t => {1}".format(request_url, response.status_code))
    response.raise_for_status()


def get_dataset_metadata(dataset: str, ckan_url: str = DATA_SOURCE_URL) -> dict:

    success, result = commons.ckan_api_request(ckan_url, endpoint="package_show", method="get",
                                               params={"id": dataset}, verbose=False)
    if success >= 0:
        ckan_dataset = result["result"]
        return ckan_dataset
    else:
        raise Exception("ERROR: Cannot retrieve dataset {} from {}".format(dataset, ckan_url))


def as_simple_text(text: str):
    simple_text = lxml.html.fromstring(text).text_content().replace('\n', "").replace('\r', "")
    return simple_text


def get_keywords(metadata: dict) -> list:
    keywords = []
    for key in metadata["tag_string_schemaorg"].split(','):
        tag = key.strip().upper()
        if tag.endswith('-ES'):
            tag = tag.rsplit('-', 1)[0]
            if tag not in keywords:
                keywords += [tag]
    for key in str(metadata['original_tags']).split(','):
        tag = key.strip().upper()
        if tag not in keywords:
            keywords += [tag]
    return keywords


def get_dataset_entities(metadata: dict, ckan_url: str = DATA_SOURCE_URL, provider_url: str = CONNECTOR_DOCKER_URL) -> dict:
    # catalog / offers / representations-artifacts
    id = metadata['id']
    source_url = metadata['url']
    organization_name = metadata['organization']['name']

    organization_metadata = {}
    success, result = commons.ckan_api_request(ckan_url, endpoint="organization_show", method="get",
                                               params={"id": organization_name}, verbose=False)
    if success >= 0:
        organization_metadata = result['result']

    catalog = {
        "title": "Catalog: " + json.loads(metadata['organization']['title'])["es"],
        "description": json.loads(metadata['organization']['description'])["es"],
        "language": "ES",
        # "additional":
        "organization_id": metadata['organization']['id'],
        "organization_name": organization_name,
        "organization_source": organization_metadata['source']
    }

    entities = {'catalog': catalog, 'offers': []}
    for resource in metadata['resources']:
        resource_id = resource['id']
        file_format = resource['format']
        data_url = resource['url']
        sample = None
        datastore_info = None
        if file_format == 'CSV':
            success, result = commons.ckan_api_request(ckan_url, endpoint="datastore_search", method="get",
                                                       params={"resource_id": resource_id}, verbose=False)
            if success >= 0:
                header = {k['id']: k['type'] for k in result['result']['fields']}
                sample = [json.dumps(header)] + [json.dumps(r) for r in result['result']['records']]
                # datastore_info = json.dumps(result['result']['fields'])
            offer = {'data': {"resource_id": "{}_{}".format(id, resource_id),
                              "resource_name": "{}_{}".format(metadata["name"], resource["name"]["es"]),
                              "title": metadata["title"]["es"] + " - " + resource["name"]["es"],
                              "description": as_simple_text(metadata["notes"]["es"]) + " " +
                                             resource["description"]["es"],
                              "keywords": get_keywords(metadata),
                              "publisher": ckan_url,
                              "language": "ES",
                              "license": metadata["license_url"],
                              "sovereign": catalog['organization_source'],
                              "endpointDocumentation": source_url,
                              "paymentMethod": None,
                              'dataset_name': metadata['name'],
                              'data_url': data_url,
                              'source_url': source_url,
                              "dataset_id": metadata['id'],
                              "dataset_url": ckan_url + '/dataset/' + metadata['name'],
                              "organization_id": catalog['organization_id'],
                              "organization_name": catalog['organization_name'],
                              },
                     'sample_data': sample}
            representation = {'data': {
                                "title": offer["data"]["title"] + " (CSV format)",
                                "description": "CSV representation of resource: " + offer["data"]["description"],
                                "mediaType": "text/csv",
                                "language": "https://w3id.org/idsa/code/ES",
                                "organization_id": offer['data']["organization_id"],
                                "organization_name": catalog['organization_name'],
                                "dataset_id": offer['data']["dataset_id"],
                                "dataset_url": ckan_url + '/dataset/' + metadata['name'],
                                "dataset_name": offer['data']["dataset_name"],
                                "resource_id": offer['data']["resource_id"],
                                "resource_name": offer['data']["resource_name"]
                            }}
            artifact = {
                            "title": offer["data"]["title"] + " (CSV data)",
                            "description": "Artifact as CSV data of resource: " + offer["data"]["description"],
                            "accessUrl": data_url,
                            "automatedDownload": True,
                            "organization_id": offer['data']["organization_id"],
                            "organization_name": catalog['organization_name'],
                            "dataset_id": offer['data']["dataset_id"],
                            "dataset_url": ckan_url + '/dataset/' + metadata['name'],
                            "dataset_name": offer['data']["dataset_name"],
                            "resource_id": offer['data']["resource_id"],
                            "resource_name": offer['data']["resource_name"]
                        }
            contract = {'data': {
                            "title": offer["data"]["title"] + " (Contract)",
                            "description": "Usage contract template for resource: " + offer["data"]["description"],
                            "provider": provider_url,
                            "organization_id": offer['data']["organization_id"],
                            "organization_name": catalog['organization_name'],
                            "dataset_id": offer['data']["dataset_id"],
                            "dataset_url": ckan_url + '/dataset/' + metadata['name'],
                            "dataset_name": offer['data']["dataset_name"],
                            "resource_id": offer['data']["resource_id"],
                            "resource_name": offer['data']["resource_name"]
                        },
                        'rule': {
                            "title": offer["data"]["title"] + " (Rule)",
                            "description": "Rule for resource: " + offer["data"]["description"],
                            "value": get_rule(),
                            "organization_id": offer['data']["organization_id"],
                            "organization_name": catalog['organization_name'],
                            "dataset_id": offer['data']["dataset_id"],
                            "dataset_url": ckan_url + '/dataset/' + metadata['name'],
                            "dataset_name": offer['data']["dataset_name"],
                            "resource_id": offer['data']["resource_id"],
                            "resource_name": offer['data']["resource_name"]
                        }
                      }
            representation["artifact"] = artifact
            offer["representations"] = [representation]
            offer["contract"] = contract
            entities['offers'] += [offer]

    return entities


def import_dataset(dataset: str, connector_url: str, auth: tuple) -> list:
    imported = []
    metadata = get_dataset_metadata(dataset)
    entities_data = get_dataset_entities(metadata)
    catalog = upsert_catalog(entities_data['catalog'], connector_url, auth)
    imported += [catalog]
    for offer_data in entities_data['offers']:
        # upsert offer
        print(" - Upsert offer: {}".format(offer_data["data"]["title"]))
        offer = upsert_offer(offer_data['data'], connector_url, auth)
        add_offer_to_catalog(offer, catalog, auth)
        print(" - Upsert contract and rule: {}".format(offer_data['contract']["data"]["title"]))
        contract = upsert_resource_entity(offer_data['contract']['data'], 'contracts', connector_url, auth)
        rule = upsert_resource_entity(offer_data['contract']['rule'], 'rules', connector_url, auth)
        add_rule_to_contract(rule, contract, auth)
        add_contract_to_offer(contract, offer, auth)
        # Add contract to offer
        for representation_data in offer_data['representations']:
            print(" - Upsert representation: {}".format(representation_data["data"]["title"]))
            representation = upsert_resource_entity(representation_data['data'], 'representations', connector_url, auth)
            artifact_data = representation_data['artifact']
            print(" - Upsert artifact: {}".format(artifact_data["title"]))
            artifact = upsert_resource_entity(artifact_data, 'artifacts', connector_url, auth)
            print(" - Add artifact to representation: {} => {}".format(artifact_data["title"],
                                                                       representation_data["data"]["title"]))
            add_artifact_to_representation(artifact, representation, auth)
            print(" - Add representation to offer: {} => {}".format(representation_data["data"]["title"],
                                                                    offer_data["data"]["title"]))
            add_representation_to_offer(representation, offer, auth)

    return imported


def post_broker_registration(metadata_broker_url, connector_url, auth) -> dict:
    request_url = "{0}/api/ids/connector/update?recipient={1}".format(connector_url, metadata_broker_url)
    response = requests.post(request_url, data={}, auth=auth, verify=False)
    print(" \t\t\t\t - Request POST connector to broker {0}\t => {1}".format(request_url, response.status_code))
    response.raise_for_status()
    return response.content


def main(metadata_broker_url: str = METADATA_BROKER_URL, metadata_broker_docker_url: str = METADATA_BROKER_DOCKER_URL,
         connector_url: str = CONNECTOR_URL, connector_docker_url: str = CONNECTOR_DOCKER_URL,
         connector_user: str = CONNECTOR_USER, connector_pw: str = CONNECTOR_PW, input_file: str = DATASET_LIST):

    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    print('Metadata Browser started... \n * Setup:')
    print('\t - METADATA_BROKER_URL: {0} ({1})'.format(metadata_broker_url, metadata_broker_docker_url))
    print('\t - CONNECTOR_URL: {0} ({1})'.format(connector_url, connector_docker_url))
    print('\t - DATASET_LIST: {0}'.format(input_file))

    connector_auth = (connector_user, connector_pw)

    datasets = get_dataset_list()
    print("\n * Importing {} datasets as resources: {}...] => OK".format(len(datasets), str(datasets)[:300]))
    imported_resources = []
    count = 1
    for dataset in datasets:
        print("\t\t - Importing dataset #{}/{}: {}...".format(count, len(datasets), dataset))
        imported_resources += import_dataset(dataset, connector_url, connector_auth)
        count += 1
        print("\t\t\t ... done!\n")
    print("\t\t ... Imported resources: {}... => OK".format(str(imported_resources)[:300]))

    print("\n * Requesting broker self-description...")
    broker_description = get_broker_description(metadata_broker_url)
    print("\t\t ... Got Broker Description: {}... => OK".format(str(broker_description)[:300]))

    print("\n * Requesting connector self-description...")
    self_description = get_self_description(connector_url, connector_auth)
    print("\t\t ... Got Self Description: {}... => OK".format(str(self_description)[:300]))

    print("\n * Register connector in the broker...")
    broker_registration = post_broker_registration(metadata_broker_docker_url, connector_url, connector_auth)
    print("\t\t ... Registered in Broker: {}... => OK".format(str(broker_registration)[:300]))

    print("\t... DONE.")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
