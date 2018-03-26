import requests
import json
import time

INGEST_API = 'http://api.ingest.integration.data.humancellatlas.org'
HEADERS = {'Content-type': 'application/json'}
TIMEOUT = 0.25


def _check_for_error(r):
    try:
        r.raise_for_status()

    except requests.HTTPError:
        print("error >>")
        _print_response(r)


def _update_state(url, state):
    r = requests.patch(url, data="{\"validationState\" : \"" + state + "\"}", headers=HEADERS)
    _check_for_error(r)

    return r


def _get_metadata_with_state(metadata_type, submission_id, state):
    url = f"{INGEST_API}/submissionEnvelopes/{submission_id}/{metadata_type}/{state}"
    print(url)

    r = requests.get(url, headers=HEADERS)
    _check_for_error(r)

    return _get_list(r.json(), metadata_type)


def _get_list(metadata, metadata_type):
    if "_embedded" in metadata:
        return metadata["_embedded"][metadata_type]
    return []


def _get_metadata_with_state_total(metadata_type, submission_id, state):
    url = f"{INGEST_API}/submissionEnvelopes/{submission_id}/{metadata_type}/{state}"
    # print(url)
    r = requests.get(url, headers=HEADERS)
    _check_for_error(r)

    return _get_total(r.json(), metadata_type)


def _get_total(metadata, metadata_type):
    if "page" in metadata:
        return metadata["page"]["totalElements"]
    return []


# generator


def _get_metadata_list(metadata_type, submission_id, state):
    url = f"{INGEST_API}/submissionEnvelopes/{submission_id}/{metadata_type}/{state}"
    return _get_all(url, metadata_type)


def _get_all(url, entity_type):
    r = requests.get(url, headers=HEADERS)
    if r.status_code == requests.codes.ok:
        if "_embedded" in json.loads(r.text):
            for entity in json.loads(r.text)["_embedded"][entity_type]:
                yield entity
            if "next" in json.loads(r.text)["_links"]:
                for entity2 in _get_all(json.loads(r.text)["_links"]["next"]["href"], entity_type):
                    yield entity2


def _has_uuid(metadata):
    return metadata["uuid"] is not None


def update_list_with_no_uuids(metadata_list):
    ctr = 0

    for index, metadata in enumerate(metadata_list):
        ctr = ctr + 1
        if not _has_uuid(metadata):
            print("no uuid")
            url = metadata["_links"]["self"]["href"]
            print(url)
            time.sleep(TIMEOUT)
            _update_state(url, 'DRAFT')

    print('ctr: ' + str(ctr))


def update_list(metadata_list):
    ctr = 0

    for index, metadata in enumerate(metadata_list):
        ctr = ctr + 1
        url = metadata["_links"]["self"]["href"]
        print(url)
        time.sleep(TIMEOUT)
        _update_state(url, 'DRAFT')

    print('ctr: ' + str(ctr))


def print_list(metadata_list):
    print('[')

    for index, metadata in enumerate(metadata_list):
        print(json.dumps(metadata) + ',')
        time.sleep(2)

    print(']')


def _print_response(res):
    print('RESPONSE:\n{status_code}\n{headers}\n\n{text}\n\n'.format(
        status_code=res.status_code,
        headers='\n'.join('{}: {}'.format(k, v) for k, v in res.headers.items()),
        text=res.text
    ))


def print_summary(submission):
    for entity in ["files", "processes", "protocols", "biomaterials"]:
        print("\n" + entity.upper())
        for state in ["Valid", "Invalid", "Validating", "Draft", '']:
            count = _get_metadata_with_state_total(entity, submission, state)
            print(state + ": " + str(count))


if __name__ == '__main__':

    SUBMISSION_ID="5ab3b8da4b3c9f0007e33c87"

    print_summary(SUBMISSION_ID)

    # files = _get_metadata_list('files', SUBMISSION_ID, 'DRAFT')
    # files = _get_metadata_list('files', SUBMISSION_ID, 'VALIDATING')

    # biomaterials = _get_metadata_list('biomaterials', SUBMISSION_ID, 'DRAFT')
    # biomaterials = _get_metadata_list('biomaterials', SUBMISSION_ID, 'VALIDATING')

    # processes = _get_metadata_list('processes', SUBMISSION_ID, 'DRAFT')
    # processes = _get_metadata_list('processes', SUBMISSION_ID, 'VALIDATING')
    # processes = _get_metadata_list('processes', SUBMISSION_ID, 'INVALID')

    # protocols = _get_metadata_list('protocols', SUBMISSION_ID, 'DRAFT')

    # update_list_with_no_uuids(files)
    # update_list_with_no_uuids(biomaterials)
    # update_list_with_no_uuids(processes)
    # update_list_with_no_uuids(protocols)

    # update_list(files)
    # update_list(biomaterials)
    # update_list(processes)
    # update_list(protocols)

    # files = _get_metadata_list('files', SUBMISSION_ID, 'VALID')
    # print_list(files)

