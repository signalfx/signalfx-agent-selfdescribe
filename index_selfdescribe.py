import copy
import json
import os.path
import subprocess
import tempfile
from urllib.parse import urlparse
from elasticsearch import Elasticsearch

import requests
from github import Github


def fetch_selfdescribe(tarball_url):
    resp = requests.get(tarball_url, allow_redirects=True)
    with tempfile.TemporaryDirectory() as tmp_dir:
        tarball_path = os.path.join(tmp_dir, urlparse(tarball_url).path.split('/')[-1])
        with open(tarball_path, 'wb') as fs:
            fs.write(resp.content)
        with subprocess.Popen(['tar', '-zxf', tarball_path, '-C', tmp_dir, "--strip=%s" % 1], stdout=subprocess.PIPE):
            pass
        selfdescribe_path = os.path.join(tmp_dir, 'selfdescribe.json')
        if os.path.isfile(selfdescribe_path):
            with open(selfdescribe_path) as fs:
                return json.load(fs)
        return {}


def create_observer_docs(sanitized_selfdescribe, release):
    observer_docs = []
    if not sanitized_selfdescribe:
        return observer_docs
    for observer in sanitized_selfdescribe['Observers']:
        observer_doc = copy.deepcopy(observer)
        observer_doc['releaseTag'] = release.tag_name
        observer_doc['publishedAt'] = release.published_at
        observer_docs.append(observer_doc)
    return observer_docs


def create_monitor_docs(sanitized_selfdescribe, release):
    monitor_docs = []
    if not sanitized_selfdescribe:
        return monitor_docs
    for monitor in sanitized_selfdescribe['Monitors']:
        monitor_doc = copy.deepcopy(monitor)
        monitor_doc['releaseTag'] = release.tag_name
        monitor_doc['publishedAt'] = release.published_at
        monitor_docs.append(monitor_doc)
    return monitor_docs


def create_metric_docs(sanitized_selfdescribe, release):
    metric_docs = []
    if not sanitized_selfdescribe:
        return metric_docs
    for monitor in sanitized_selfdescribe['Monitors']:
        for metric in monitor['metrics'] or {}:
            metric_doc = copy.deepcopy(monitor['metrics'][metric])
            metric_doc['metric'] = metric
            metric_doc['releaseTag'] = release.tag_name
            metric_doc['publishedAt'] = release.published_at
            metric_doc['monitorType'] = monitor['monitorType']
            metric_doc['dimensions'] = monitor['dimensions']
            metric_doc['properties'] = monitor['properties']
            metric_docs.append(metric_doc)
    return metric_docs


def create_dimension_docs_monitor_defined(sanitized_selfdescribe, release):
    dimension_docs = []
    if not sanitized_selfdescribe:
        return dimension_docs
    for monitor in sanitized_selfdescribe['Monitors']:
        for dimension in monitor['dimensions'] or []:
            dimension_doc = {
                'dimension': dimension,
                'releaseTag': release.tag_name,
                'publishedAt': release.published_at,
                'monitorType': monitor['monitorType'],
                'metrics': monitor['metrics'],
                'properties': {}
            }
            for prop in monitor['properties'] or {}:
                if dimension in monitor['properties'][prop]['dimension']:
                    dimension_doc['properties'][prop] = monitor['properties'][prop]
            dimension_docs.append(dimension_doc)
    return dimension_docs


def create_dimension_docs_observer_defined(sanitized_selfdescribe, release):
    dimension_docs = []
    if not sanitized_selfdescribe:
        return dimension_docs
    for observer in sanitized_selfdescribe['Observers']:
        for dimension in observer['dimensions'] or []:
            dimension_doc = {
                'dimension': dimension,
                'releaseTag': release.tag_name,
                'publishedAt': release.published_at,
                'observerType': observer['observerType']
            }
            dimension_docs.append(dimension_doc)
    return dimension_docs


def create_property_docs(sanitized_selfdescribe, release):
    property_docs = []
    if not sanitized_selfdescribe:
        return property_docs
    for monitor in sanitized_selfdescribe['Monitors']:
        for prop in monitor['properties'] or {}:
            for dimension in monitor['dimensions'] or {None: None}:
                if dimension in monitor['properties'][prop]['dimension']:
                    property_doc = {
                        'property': prop,
                        'releaseTag': release.tag_name,
                        'publishedAt': release.published_at,
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


def index_selfdescribe():
    # github personal access token agent-download-test
    g = Github(login_or_token=os.environ.get('GITHUB_PERSONAL_ACCESS_TOKEN'))
    repo = g.get_repo('signalfx/signalfx-agent')
    selfdescribes, agent_releases = {}, {}
    for agent_release in repo.get_releases():
        agent_releases[agent_release.tag_name] = agent_release
        selfdescribes[agent_release.tag_name] = sanitize(fetch_selfdescribe(agent_release.tarball_url))

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
        for release_tag in selfdescribes:
            for doc in indices[index](selfdescribes[release_tag], agent_releases[release_tag]):
                es.index(index=index, body=doc)


if __name__ == '__main__':
    index_selfdescribe()
