import os
from datetime import datetime, timezone

import paramiko
from scp import SCPClient
import sqlite3

'''
    sqlite data pipeline requires a local copy to be made
'''

class AccessSqliteDB:
    def __init__(
        self,
        sqlite_remote_server_ip,
        sqlite_remote_server_user,
        sqlite_remote_server_pass,
        sqlite_remote_server_file_path,
        sqlite_local_destination,
        private_key_path=None,
    ):

        self.sqlite_remote_server_ip = sqlite_remote_server_ip
        self.sqlite_remote_server_user = sqlite_remote_server_user
        self.sqlite_remote_server_pass = sqlite_remote_server_pass

        self.sqlite_remote_server_file_path = sqlite_remote_server_file_path
        self.sqlite_local_destination = sqlite_local_destination

        if private_key_path:
            self.private_key = paramiko.Ed25519Key.from_private_key_file(private_key_path)
        else:
            self.private_key = None

    def copy_sqlite_db(
        self,
        method='password',
        rename='sqlite_db.sqlite',
    ):

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if method == 'password':
            ssh.connect(
                hostname=self.sqlite_remote_server_ip,
                username=self.sqlite_remote_server_user,
                password=self.sqlite_remote_server_pass,
                port=22,
            )
        elif method == 'private_key' and self.private_key:
            ssh.connect(
                hostname=self.sqlite_remote_server_ip,
                username=self.sqlite_remote_server_user,
                pkey=self.private_key,
                port=22,
            )

        if not os.path.exists(self.sqlite_local_destination):
            os.makedirs(self.sqlite_local_destination)

        with SCPClient(ssh.get_transport()) as scp:
            db_path = os.path.join(self.sqlite_local_destination, rename)
            scp.get(self.sqlite_remote_server_file_path, db_path)
        
        ssh.close()

        return db_path

class SQLiteCall():
    def __init__(
        self,
        db_path,
        test=False,
        test_limit=1,
    ):
    
        self.db_path = db_path
        self.test = test
        if test:
            if not test_limit or test_limit < 1:
                self.test_limit = 1
            else:
                self.test_limit = test_limit

    def yield_records(
        self,
        object_name,
        where_clause=None,
        incremental:dict=None,
        processed_timestamp=datetime.now(timezone.utc),
    ):

        if where_clause and not incremental:
            pass # -> where_clause = where_clause
        elif not where_clause and incremental:
            where_clause = f''' WHERE {incremental['attribute']} > '{incremental['value']}' '''
        elif where_clause and incremental:
            where_clause = where_clause + f''' AND {incremental['attribute']} > '{incremental['value']}' '''
        else:
            where_clause = ''

        if self.test:
            limit_clause = f' LIMIT {self.test_limit}' if self.test else ''
        else:
            limit_clause = ''


        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()


        cursor.execute(f'''SELECT *, cast('{processed_timestamp}' as timestamp) as _dlt_processed_utc FROM {object_name} {where_clause} {limit_clause}''')
        columns = [ col[0] for col in cursor.description ]

        for row in cursor.fetchall():
            yield dict(zip(columns, row))

        conn.close()


if __name__ == '__main__':
    pass
