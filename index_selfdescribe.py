import base64
import copy
import glob
import json
import os.path
import subprocess
import tempfile
from urllib.parse import urlparse
from elasticsearch import Elasticsearch

import requests
from github import Github


# def fetch_selfdescribe(tarball_url):
#     resp = requests.get(tarball_url, allow_redirects=True)
#     with tempfile.TemporaryDirectory() as tmp_dir:
#         tarball_path = os.path.join(tmp_dir, urlparse(tarball_url).path.split('/')[-1])
#         with open(tarball_path, 'wb') as fs:
#             fs.write(resp.content)
#         with subprocess.Popen(['tar', '-zxf', tarball_path, '-C', tmp_dir, "--strip=%s" % 1], stdout=subprocess.PIPE):
#             pass
#         selfdescribe_path = os.path.join(tmp_dir, 'selfdescribe.json')
#         if os.path.isfile(selfdescribe_path):
#             with open(selfdescribe_path) as fs:
#                 return json.load(fs)
#         return {}


def create_observer_docs(sanitized_selfdescribe, release_tag, sha):
    observer_docs = []
    if not sanitized_selfdescribe:
        return observer_docs
    for observer in sanitized_selfdescribe['Observers']:
        observer_doc = copy.deepcopy(observer)
        observer_doc['releaseTag'] = release_tag
        observer_doc['sha'] = sha
        observer_docs.append(observer_doc)
    return observer_docs


def create_monitor_docs(sanitized_selfdescribe, release_tag, sha):
    monitor_docs = []
    if not sanitized_selfdescribe:
        return monitor_docs
    for monitor in sanitized_selfdescribe['Monitors']:
        monitor_doc = copy.deepcopy(monitor)
        monitor_doc['releaseTag'] = release_tag
        monitor_doc['sha'] = sha
        monitor_docs.append(monitor_doc)
    return monitor_docs


def create_metric_docs(sanitized_selfdescribe, release_tag, sha):
    metric_docs = []
    if not sanitized_selfdescribe:
        return metric_docs
    for monitor in sanitized_selfdescribe['Monitors']:
        for metric in monitor['metrics'] or {}:
            metric_doc = copy.deepcopy(monitor['metrics'][metric])
            metric_doc['metric'] = metric
            metric_doc['releaseTag'] = release_tag
            metric_doc['sha'] = sha
            metric_doc['monitorType'] = monitor['monitorType']
            metric_doc['dimensions'] = monitor['dimensions']
            metric_doc['properties'] = monitor['properties']
            metric_docs.append(metric_doc)
    return metric_docs


def create_dimension_docs_monitor_defined(sanitized_selfdescribe, release_tag, sha):
    dimension_docs = []
    if not sanitized_selfdescribe:
        return dimension_docs
    for monitor in sanitized_selfdescribe['Monitors']:
        for dimension in monitor['dimensions'] or []:
            dimension_doc = {
                'dimension': dimension,
                'releaseTag': release_tag,
                'sha': sha,
                'monitorType': monitor['monitorType'],
                'metrics': monitor['metrics'],
                'properties': {}
            }
            for prop in monitor['properties'] or {}:
                if dimension in monitor['properties'][prop]['dimension']:
                    dimension_doc['properties'][prop] = monitor['properties'][prop]
            dimension_docs.append(dimension_doc)
    return dimension_docs


def create_dimension_docs_observer_defined(sanitized_selfdescribe, release_tag, sha):
    dimension_docs = []
    if not sanitized_selfdescribe:
        return dimension_docs
    for observer in sanitized_selfdescribe['Observers']:
        for dimension in observer['dimensions'] or []:
            dimension_doc = {
                'dimension': dimension,
                'releaseTag': release_tag,
                'sha': sha,
                'observerType': observer['observerType']
            }
            dimension_docs.append(dimension_doc)
    return dimension_docs


def create_property_docs(sanitized_selfdescribe, release_tag, sha):
    property_docs = []
    if not sanitized_selfdescribe:
        return property_docs
    for monitor in sanitized_selfdescribe['Monitors']:
        for prop in monitor['properties'] or {}:
            for dimension in monitor['dimensions'] or {None: None}:
                if dimension in monitor['properties'][prop]['dimension']:
                    property_doc = {
                        'property': prop,
                        'releaseTag': release_tag,
                        'sha': sha,
                        'monitorType': monitor['monitorType'],
                        'dimension': dimension
                    }
                    property_docs.append(property_doc)
    return property_docs


def sanitize(selfdescribe):
    if not selfdescribe:
        return selfdescribe
    selfdescribe_copy = copy.deepcopy(selfdescribe)
    for field in ['GenericMonitorConfig', 'GenericObserverConfig', 'SourceConfig', 'TopConfig']:
        selfdescribe_copy.pop(field, None)
    for i in range(len(selfdescribe['Observers'])):
        for field in ['name', 'doc', 'package', 'fields', 'endpointVariables']:
            selfdescribe_copy['Observers'][i].pop(field, None)
        selfdescribe_copy['Observers'][i]['dimensions'] = []
        for dimension in selfdescribe['Observers'][i]['dimensions']:
            selfdescribe_copy['Observers'][i]['dimensions'].append(dimension if type(dimension) is str else dimension['name'])
    for i in range(len(selfdescribe['Monitors'])):
        for field in ['config', 'doc', 'groups', 'metrics', 'name', 'package', 'fields']:
            selfdescribe_copy['Monitors'][i].pop(field, None)
        selfdescribe_copy['Monitors'][i]['metrics'] = {}
        for metric in selfdescribe['Monitors'][i]['metrics'] or {}:
            name = metric if type(metric) is str else metric['name']
            if type(metric) is str:
                selfdescribe_copy['Monitors'][i]['metrics'][name] = dict(selfdescribe['Monitors'][i]['metrics'][name])
            else:
                selfdescribe_copy['Monitors'][i]['metrics'][name] = dict(metric)
                selfdescribe_copy['Monitors'][i]['metrics'][name].pop('name', None)
            if 'included' in selfdescribe_copy['Monitors'][i]['metrics'][name]:
                selfdescribe_copy['Monitors'][i]['metrics'][name]['default'] = selfdescribe_copy['Monitors'][i]['metrics'][name]['included']
            selfdescribe_copy['Monitors'][i]['metrics'][name].pop('included', None)
        selfdescribe_copy['Monitors'][i]['dimensions'] = []
        for dimension in selfdescribe['Monitors'][i]['dimensions'] or {}:
            selfdescribe_copy['Monitors'][i]['dimensions'].append(dimension if type(dimension) is str else dimension['name'])
        selfdescribe_copy['Monitors'][i]['properties'] = {}
        for prop in selfdescribe['Monitors'][i]['properties'] or {}:
            name = prop if type(prop) is str else prop['name']
            if type(prop) is str:
                selfdescribe_copy['Monitors'][i]['properties'][name] = dict(selfdescribe['Monitors'][i]['properties'][name])
            else:
                selfdescribe_copy['Monitors'][i]['properties'][name] = dict(prop)
                selfdescribe_copy['Monitors'][i]['properties'][name].pop('name', None)
            selfdescribe_copy['Monitors'][i]['properties'][name].pop('description', None)
    return selfdescribe_copy


def download_selfdescribe(download_path, repo, sha):
    contents = repo.get_contents("", ref=sha)
    for content in contents:
        if content.path == "selfdescribe.json":
            blob = repo.get_git_blob(content.sha)
            data = base64.b64decode(blob.content)
            with open(download_path, "w") as out:
                out.write(data.decode("utf-8"))
            break


def index_selfdescribe(repo_name, token, sha_list):
    # # github personal access token agent-download-test
    g = Github(login_or_token=token)
    repo = g.get_repo(repo_name)
    tags = {tag.commit.sha: tag.name for tag in repo.get_tags()}
    for sha in sha_list:
        download_dir = os.path.join("downloads", tags.get(sha, '_'), sha)
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        download_path = os.path.join(download_dir, "selfdescribe.json")
        if not os.path.exists(download_path):
            download_selfdescribe(download_path, repo, sha)

    paths = [f for f in glob.glob("downloads/**/selfdescribe.json", recursive=True)]

    # for path in paths:
    #     with open(path) as fs:
    #         sha = os.path.split(os.path.split(path)[0])[1]
    #         tag = os.path.split(os.path.split(os.path.split(path)[0])[0])[1]

    # selfdescribes, agent_releases = {}, {}
    # for agent_release in repo.get_releases():
    #     agent_releases[agent_release.tag_name] = agent_release
    #     selfdescribes[agent_release.tag_name] = sanitize(fetch_selfdescribe(agent_release.tarball_url))

    es = Elasticsearch()
    settings = {
        "settings": {
            "index.mapping.total_fields.limit": 100000
        },
        # "mappings": {
        #     "dynamic_templates": [
        #         {
        #             "fields_defaults_as_text": {
        #                 "path_match": "*fields.default",
        #                 "mapping": {
        #                     "type": "keyword"
        #                 }
        #             }
        #         }
        #     ]
        #  }
    }
    indices = {
        'selfdescribe-observers': create_observer_docs,
        'selfdescribe-monitors': create_monitor_docs,
        'selfdescribe-metrics': create_metric_docs,
        'selfdescribe-dimensions-observer-defined': create_dimension_docs_observer_defined,
        'selfdescribe-dimensions-monitor-defined': create_dimension_docs_monitor_defined,
        'selfdescribe-properties': create_property_docs,
    }
    for index in indices:
        if es.indices.exists([index]):
            es.indices.delete(index=[index])
        es.indices.create(index=index, body=settings)
        for path in paths:
            with open(path) as fs:
                sha = os.path.split(os.path.split(path)[0])[1]
                release_tag = os.path.split(os.path.split(os.path.split(path)[0])[0])[1]
                for doc in indices[index](sanitize(json.load(fs)), release_tag, sha):
                    es.index(index=index, body=doc)

        # for release_tag in selfdescribes:
        #     for doc in indices[index](selfdescribes[release_tag], agent_releases[release_tag]):
        #         es.index(index=index, body=doc)
