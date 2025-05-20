import datetime
import hashlib
import os
import uuid
from json import dumps
from urllib.parse import urljoin

import clickhouse_connect
import requests

VISIOLOGY_URL = os.environ['VISIOLOGY_URL']
VISIOLOGY_LOGIN = os.environ['VISIOLOGY_LOGIN']
VISIOLOGY_PASSWORD = os.environ['VISIOLOGY_PASSWORD']

CLICKHOUSE_HOST = os.environ['CLICKHOUSE_HOST']
CLICKHOUSE_PORT = int(os.environ['CLICKHOUSE_PORT'])
CLICKHOUSE_DATABASE = os.environ['CLICKHOUSE_DATABASE']
CLICKHOUSE_LOGIN = os.environ['CLICKHOUSE_LOGIN']
CLICKHOUSE_PASSWORD = os.environ['CLICKHOUSE_PASSWORD']

HASH_USERS = (os.environ['HASH_USERS'] == 'Y')


def get_auth_header(server, username, password):
    data = {
        'client_id': 'visiology_designer',
        'grant_type': 'password',
        'scope': 'openid data_management_service formula_engine workspace_service dashboard_service',
        'username': username,
        'password': password,
    }
    response = requests.post(urljoin(server, 'keycloak/realms/Visiology/protocol/openid-connect/token'),
                             data=data,
                             verify=False)
    response_data = response.json()
    headers = {
        'Authorization': f'Bearer {response_data["access_token"]}',
    }
    return headers


def get_workspaces(server, username, password, col_order=['id', 'name', 'createdAt', 'createdBy', 'date']):
    data = requests.get(urljoin(server, 'workspace-service/api/v1/workspaces'),
                        headers=get_auth_header(server, username, password),
                        verify=False).json()
    for d in data:
        d['id'] = uuid.UUID(d['id'])
        d['createdAt'] = datetime.datetime.strptime(d['createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ')
        d['date'] = datetime.date.today()
    return [[d[c] for c in col_order] for d in data]


def get_dashboards(server, username, password, workspace_id=None,
                   col_order=['guid', 'name', 'lastModified', 'lastEditorName', 'dataset', 'isPublic',
                              'publishedOnPortal', 'workspace_id', 'date']):
    if workspace_id is not None:
        ws_ids = [workspace_id]
    else:
        ws_ids = [str(ws[0]) for ws in get_workspaces(server, username, password)]

    result = []

    for ws in ws_ids:
        dbs = requests.get(
            urljoin(server,
                    f'dashboard-service/api/workspaces/{ws}/dashboards'),
            headers=get_auth_header(server, username, password), verify=False).json()
        for db in dbs:
            result.append(db.update({'workspace_id': ws}) or db)
    for d in result:
        d['guid'] = uuid.UUID(d['guid'])
        d['lastModified'] = datetime.datetime.strptime(d['lastModified'], '%Y-%m-%dT%H:%M:%S.%fZ')
        d['dataset'] = dumps(d['dataset'])
        d['workspace_id'] = uuid.UUID(d['workspace_id'])
        d['date'] = datetime.date.today()
    return [[d[c] for c in col_order] for d in result]


def get_widgets(server, username, password, workspace_id=None, dashboard_id=None,
                col_order=['workspace_id', 'dashboard_id', 'sheet_guid', 'sheet_name',
                           'sheet_position', 'widget_type', 'widget_label', 'widget_guid',
                           'widget_date']):
    if workspace_id is not None:
        ws_ids = [workspace_id]
    else:
        ws_ids = [str(ws[0]) for ws in get_workspaces(server, username, password)]

    dashboards = []

    for ws in ws_ids:
        dbs = requests.get(
            urljoin(server,
                    f'dashboard-service/api/workspaces/{ws}/dashboards'),
            headers=get_auth_header(server, username, password), verify=False).json()
        dbs = [[ws, db['guid']] for db in dbs if dashboard_id is None or db['guid'] == dashboard_id]
        if dbs:
            dashboards += dbs

    result = []

    for ws, db in dashboards:
        db_struct = requests.get(
            urljoin(server,
                    f'dashboard-service/api/workspaces/{ws}/dashboards/{db}'),
            headers=get_auth_header(server, username, password), verify=False).json()

        for sheet in db_struct['sheets']:
            sheet_guid = sheet['guid']
            sheet_name = sheet['name']
            sheet_position = sheet['position']

            for widget in sheet['widgets']:
                widget_type = widget['type']
                widget_label = widget['title']['text']
                widget_guid = widget['guid']
                result.append({
                    'workspace_id': uuid.UUID(ws),
                    'dashboard_id': uuid.UUID(db),
                    'sheet_guid': uuid.UUID(sheet_guid),
                    'sheet_name': sheet_name,
                    'sheet_position': sheet_position,
                    'widget_type': widget_type,
                    'widget_label': widget_label,
                    'widget_guid': uuid.UUID(widget_guid),
                    'widget_date': datetime.date.today(),
                })
    return [[d[c] for c in col_order] for d in result]


def get_roles(server, username, password, workspace_id=None, hash_users=True,
              col_order=['username', 'id', 'subject_type', 'assigned_role', 'workspace_id', 'date']):
    if workspace_id is not None:
        ws_ids = [workspace_id]
    else:
        ws_ids = [str(ws[0]) for ws in get_workspaces(server, username, password)]

    result = []

    for ws in ws_ids:
        usrs = requests.get(
            urljoin(server,
                    f'workspace-service/api/v1/workspaces/{ws}/role-mappings'),
            headers=get_auth_header(server, username, password), verify=False).json()
        for usr in usrs:
            result.append({
                'workspace_id': uuid.UUID(ws),
                'username': ((sha256_hash := hashlib.new('sha256')).update(
                    usr['username'].encode()) or sha256_hash.hexdigest()) if hash_users else usr['username'],
                'id': uuid.UUID(usr['id']),
                'subject_type': usr['subjectType'],
                'assigned_role': usr['assignedRole'],
                'date': datetime.date.today()
            })
    result = [[d[c] for c in col_order] for d in result]
    return result


if __name__ == '__main__':
    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST,
                                           port=CLICKHOUSE_PORT,
                                           username=CLICKHOUSE_LOGIN,
                                           password=CLICKHOUSE_PASSWORD,
                                           database=CLICKHOUSE_DATABASE,
                                           secure=False)

    # workspaces
    client.insert(table='workspaces',
                  data=get_workspaces(VISIOLOGY_URL, VISIOLOGY_LOGIN, VISIOLOGY_PASSWORD))
    client.query('''OPTIMIZE TABLE workspaces FINAL''')

    # dashboards
    client.insert(table='dashboards',
                  data=get_dashboards(VISIOLOGY_URL, VISIOLOGY_LOGIN, VISIOLOGY_PASSWORD))
    client.query('''OPTIMIZE TABLE dashboards FINAL''')

    # widgets
    client.insert(table='widgets',
                  data=get_widgets(VISIOLOGY_URL, VISIOLOGY_LOGIN, VISIOLOGY_PASSWORD))
    client.query('''OPTIMIZE TABLE widgets FINAL''')

    # roles
    client.insert(table='roles', data=get_roles(server=VISIOLOGY_URL,
                                                username=VISIOLOGY_LOGIN,
                                                password=VISIOLOGY_PASSWORD,
                                                hash_users=HASH_USERS))
    client.query('''OPTIMIZE TABLE roles FINAL''')

    # aggregates
    client.query('''OPTIMIZE TABLE aggregates FINAL''')
