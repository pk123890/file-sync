import pymysql
from paramiko import SSHClient
from pathlib import Path
import socket
import argparse
import time
import logging
import requests
import json

logger = logging.getLogger(__name__)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def execute_sql(cursor, query, parameters):
    results = []
    try:
        if parameters is None:
            cursor.execute(query)
        else:
            cursor.execute(query, parameters)
        query_result = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        if query_result:
            for row in query_result:
                results.append(dict(zip(columns, row)))
    except Exception as e:
        logger.error(f'Exception occurred while executing sql {query}', exc_info=True)
    finally:
        return results


def sync_files_from_db():
    connection = pymysql.connect(host=db_host, user=db_user,
                                 password=db_password,
                                 database=db_name)

    cursor = connection.cursor()
    logger.info(f'Running file sync script on {local_server_name}')
    offset_id = execute_sql(cursor, f'select offset_id from {application_name}_offset_tracker where server_name = %s ',
                            local_server_name)
    is_offset_present = True
    if len(offset_id) == 0:
        offset_id = 0
        is_offset_present = False
    else:
        offset_id = offset_id[0].get('offset_id')
    logger.info(f'last offset_id from db is {offset_id}')
    if file_path_list is not None and len(file_path_list) != 0:
        file_path_query = ''
        for i in range(len(file_path_list) - 1):
            file_path_query = file_path_query + f"file_path like '%{file_path_list[i]}%' or "
        file_path_query = file_path_query + f"file_path like '%{file_path_list[len(file_path_list) - 1]}%'"
        change_log_list = execute_sql(cursor,
                                      f"select offset_id, file_path, file_owner from {application_name}_change_log "
                                      f"where file_owner != '{local_server_name}' and offset_id > {offset_id} and "
                                      f"({file_path_query}) order by offset_id ", None)
    else:
        change_log_list = execute_sql(cursor,
                                      f'select offset_id, file_path, file_owner from {application_name}_change_log '
                                      f'where file_owner != %s and offset_id > %s order by '
                                      'offset_id', (local_server_name, offset_id))
    server_set = {}
    latest_offset_id = offset_id
    logger.info(f'Last offset_id synced in {local_server_name} is {latest_offset_id}')
    upsert_query = f'Update {application_name}_offset_tracker set offset_id= %s , last_updated_at= now() where ' \
                   'server_name= %s '

    for change_log in change_log_list:
        server_set.setdefault(change_log.get('file_owner'), []).append(
            {'file_path': change_log.get('file_path'), 'offset_id': change_log.get('offset_id')})
    for server_name, path_list in server_set.items():
        try:
            ssh = SSHClient()
            ssh.load_system_host_keys()
            logger.info(f'Syncing {len(path_list)} files from {server_name} in {local_server_name} ')
            ssh.connect(hostname=server_name, username=server_user)
            with ssh.open_sftp() as scp:
                for path in path_list:
                    try:
                        dir = path.get('file_path').rsplit('/', 1)[0]
                        Path(str(dir)).mkdir(parents=True, exist_ok=True)
                        scp.get(path.get('file_path'), path.get('file_path'))
                        latest_offset_id = path.get('offset_id')
                    except Exception as ex:
                        slack_alert(f'File sync failed for server {server_name} for path {path}')
                        logger.error(f'Exception occurred while fetching file from {server_name} for path {path}',
                                      exc_info=True)
                        break
            logger.info(f'Last offset_id synced in {local_server_name} from {server_name} is {latest_offset_id}')

        except Exception as ex:
            slack_alert(f'Exception occurred while processing synchronisation for {server_name}')
            logger.error(f'Exception occurred while processing synchronisation for {server_name}', exc_info=True)
    if not is_offset_present:
        logger.info(f"Offset not present in DB for server: {local_server_name}, inserting offset entry")
        upsert_query = f'insert into {application_name}_offset_tracker(offset_id, last_updated_at, server_name) ' \
                       'values (%s, now(), %s) '
    param = (latest_offset_id, local_server_name)
    cursor.execute(upsert_query, param)
    connection.commit()
    logging.info("Successfully executed file sync script")


def slack_alert(error_message):
    url = slack_webhook
    payload = json.dumps({
        "attachments": [{"pretext": error_message,
                         "title": "FILE SYNC FAILED",
                         "text": f'File Synchronisation failed for {application_name}',
                         "color": "#ff3300"}]
    })
    headers = {
        'Content-Type': 'application/json'
    }

    requests.request("POST", url, headers=headers, data=payload)


local_server_name = socket.gethostname()

parser = argparse.ArgumentParser(description='arguments for file sync script')
parser.add_argument('--application_name', type=str)
parser.add_argument('--db_host', type=str)
parser.add_argument('--db_user', type=str)
parser.add_argument('--db_password', type=str)
parser.add_argument('--db_name', type=str)
parser.add_argument('--slack_webhook', type=str)
parser.add_argument('--file_path_list', nargs='+')
parser.add_argument('--server_user', type=str)

args = parser.parse_args()
application_name = args.application_name
db_host = args.db_host
db_user = args.db_user
db_password = args.db_password
db_name = args.db_name
slack_webhook = args.slack_webhook
file_path_list = args.file_path_list
server_user = args.server_user
if server_user is None:
    server_user = 'ec2-user'

now = time.time()

sync_files_from_db()

logger.info(f'Time taken on server {local_server_name} for File Sync {(time.time() - now)}s')

sample_run = """python3 file_synchronizer.py --application_name livechat_analytics --db_host mariadb.dev.engati.local 
--db_user live_chat_analytics --db_password live_chat_analytics --db_name LIVE_CHAT_ANALYTICS --slack_webhook 
https://hooks.slack.com/services/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 
--file_path_list 
/opt/engati/local_livechat_parquet/cs_request_history/76149/ 
/opt/engati/local_livechat_parquet/cs_request_history/73991/ 
/opt/engati/local_livechat_parquet/cs_request_history/88407/  --server_user ec2-user"""
