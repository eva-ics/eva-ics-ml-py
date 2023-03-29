__version__ = '0.0.1'

import sys
import requests
import gzip
import logging
import time
import json
import busrt
import pandas as pd

from datetime import datetime
from typing import Union
from io import StringIO, BytesIO

from evaics.client import HttpClient
from evaics.sdk import OID, FunctionFailed, pack, unpack

from busrt.rpc import Rpc as BusRpcClient

BUS_BULK_SIZE = 10_000


class HistoryDF:
    """
    Fetches data from EVA ICS v4 history databases

    When the primary module is imported, the method "history_df()" is
    automatically added to evaics.client.HttpClient
    """

    def __init__(self, client, csv: str = None):
        """
        Create HistoryDF object instance

        Args:
            client: HTTP client object

        All configuration methods of the class can be used in chains, e.g.
        HistoryDF(client).oid(
            'sensor:s1', status=False, value='s1').oid(
            'sensor:s2', status=False, value='s2').fill('1T').fetch()
        """
        self.client = client
        self.mlkit = None
        self.params = {'fill': '1S'}
        self.oid_map = {}

    def with_mlkit(self, mlkit: Union[bool, str]):
        """
        Set ML kit url/svc name

        Args:
            mlkit: True for the same URL as HMI, svc name or url for other
        """
        self.mlkit = mlkit
        return self

    def read_params_csv(self, f: str):
        """
        Read OID mapping from a CSV file

        CSV file must have the column "oid" and optional ones "status", "value"
        and "database"

        Args:
            f: file path or buffer
        """
        params = pd.read_csv(f)
        for _, row in params.iterrows():
            self.oid(row['oid'],
                     status=row.get('status'),
                     value=row.get('value'),
                     database=row.get('database'))
        return self

    def oid(self,
            oid: Union[OID, str],
            status=False,
            value=False,
            database=None,
            xopts=None):
        """
        Append OID for processing

        Args:
            oid: item OID (string or OID object)

        Optional:
            status: True to keep, a string to rename, False to drop
            value: same behavior as for keep
            database: db service to query data from (mlkit srv only)
            xopts: db service extra opts (mlkit srv only)
        """
        if status != status or \
                status == 'false' or status == '0' or status == 0:
            status = None
        elif status == 'true' or status == '1' or status == 1:
            status = True
        if value != value or \
                value == 'false' or value == '0' or value == 0:
            value = None
        elif value == 'true' or value == '1' or value == 1:
            value = True
        m = {'status': status, 'value': value}
        if database is not None:
            m['database'] = database
        if xopts is not None:
            m['xopts'] = xopts
        self.oid_map[oid] = m
        return self

    def t_start(self, t_start: Union[float, str, datetime]):
        """
        Specify the data frame start time

        Args:
            t_start: a float (timestamp), a string or a datetime object
        """
        if isinstance(t_start, datetime):
            t_start = t_start.timestamp()
        self.params['t_start'] = t_start
        return self

    def t_end(self, t_end: Union[float, str]):
        """
        Specify the data frame end time

        Args:
            t_start: a float (timestamp), a string or a datetime object
        """
        self.params['t_end'] = t_end
        return self

    def fill(self, fill: str):
        """
        Fill the data frame

        Args:
            fill: XN, where X - integer, N - fill type (S for seconds, T for
                  minutes, H for hours, D for days, W for weeks), e.g. 15T for
                  15 minutes. The values can be rounded to digits after comma
                  as XN:D, e.g 15T:2 - round to 2 digits after comma. The
                  default fill is 1S
        """
        self.params['fill'] = fill
        return self

    def limit(self, limit: int):
        """
        Limit the data frame rows to

        Args:
            limit: max number of rows
        """
        self.params['limit'] = limit
        return self

    def xopts(self, xopts: dict):
        """
        Extra database options

        Args:
            xopts: dict of extra options (refer to the EVA ICS database service
            documentation for more info)
        """
        if not self._is_mlkit_enabled():
            self.params['xopts'] = xopts
        for _, v in self.oid_map.items():
            v['xopts'] = xopts
        return self

    def database(self, database: str):
        """
        Specify a non-default database

        Args:
            database: database name (db service without eva.db. prefix)
        """
        if not self._is_mlkit_enabled():
            self.params['database'] = database
        for _, v in self.oid_map.items():
            v['database'] = database
        return self

    def _prepare_df_time(self, df, tz=None):
        if tz is None:
            tz = time.tzname[0]
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert(tz)

    def _get_mlkit(self):
        if self.mlkit is None:
            try:
                return self.client.mlkit
            except AttributeError:
                return None
        else:
            return self.mlkit

    def _is_mlkit_enabled(self):
        return self._get_mlkit() is not None

    def _get_ml_url(self):
        ml_url = self._get_mlkit()
        if ml_url is None:
            return None
        else:
            if ml_url is True:
                ml_url = self.client.url
            while ml_url.endswith('/'):
                ml_url = ml_url[:-1]
        return ml_url

    def _post(self, *args, accept_gzip=False, **kwargs):
        if not self.client.token:
            self.client.authenticate()
            refreshed = True
        else:
            refreshed = False
        while True:
            headers = {
                'x-auth-key': self.client.token,
            }
            if accept_gzip:
                headers['accept-encoding'] = 'gzip'
            req = requests.post(*args,
                                headers=headers,
                                timeout=self.client.timeout,
                                **kwargs)
            if req.status_code == 403 and req.text.endswith('(AUTH)'):
                if refreshed:
                    break
                else:
                    self.client.authenticate()
                    refreshed = True
            else:
                break
        return req

    def push(self, data, database='default'):
        """
        Push data

        Requires ML kit server

        Options:
            data: Pandas data frame, file object or file path
            database: database svc id (default: default)
        """
        if isinstance(self.client, BusRpcClient):
            raise RuntimeError('push via BUS/RT is not supported')
        else:
            ml_url = self._get_ml_url()
            if ml_url is None:
                raise RuntimeError('mlkit server not specified')
            params = {'database': database, 'oid_map': self.oid_map}
            files = {}
            if hasattr(data, 'seek'):
                files['file'] = data
            elif isinstance(data, str):
                files['file'] = open(data, 'r')
            elif isinstance(data, pd.DataFrame):
                buf = StringIO()
                data.to_csv(buf)
                buf.seek(0)
                files['file'] = buf
            else:
                raise ValueError('unsupported data kind')
            req = self._post(f'{ml_url}/ml/api/push.item.state_history_csv',
                             data={'params': json.dumps(params)},
                             files=files)
            if req.ok:
                return req.json()
            else:
                raise FunctionFailed(f'{req.status_code} {req.text}')

    def _prepare_mlkit_params(self, t_col):
        params = self.params.copy()
        params['oid_map'] = self.oid_map
        if t_col == 'drop':
            params['time_format'] = 'no'
        else:
            params['time_format'] = 'raw'
        return params

    def fetch(self, t_col: str = 'keep', tz: str = None):
        """
        Fetch data

        Optional:
            t_col: time column processing, "keep" - keep the column, "drop" -
                   drop the time column, "index" - keep and make the data frame
                   index
            tz: time zone

        Returns:
            a prepared Pandas DataFrame object
        """
        params = self.params.copy()
        if isinstance(self.client, BusRpcClient):
            params = self._prepare_mlkit_params(t_col)
            res = self.client.call(
                self._get_mlkit(),
                busrt.rpc.Request('Citem.state_history',
                                  pack(params))).wait_completed()
            cursor_id = unpack(res.get_payload(), raw=False)['u']
            cpl = pack(dict(u=cursor_id, c=BUS_BULK_SIZE))
            df = pd.DataFrame()
            while True:
                res = self.client.call(self._get_mlkit(),
                                       busrt.rpc.Request('NB',
                                                         cpl)).wait_completed()
                payload = res.get_payload()
                if payload:
                    if len(df.columns):
                        a = pd.read_csv(BytesIO(payload),
                                        header=None,
                                        names=df.columns)
                        df = pd.concat([df, a])
                        if len(a) < BUS_BULK_SIZE:
                            break
                    else:
                        df = pd.read_csv(BytesIO(payload))
                        if len(
                                df
                        ) < BUS_BULK_SIZE - 1:  # df rows (strings) + header (1 string)
                            break
                else:
                    break
            if t_col != 'drop':
                self._prepare_df_time(df, tz)
        else:
            ml_url = self._get_ml_url()
            if ml_url:
                params = self._prepare_mlkit_params(t_col)
                req = self._post(f'{ml_url}/ml/api/query.item.state_history',
                                 json=params,
                                 stream=True)
                if req.ok:
                    if req.headers.get('content-encoding') == 'gzip':
                        stream = gzip.GzipFile(fileobj=req.raw, mode="rb")
                    else:
                        stream = req.raw
                    df = pd.read_csv(stream)
                    if t_col != 'drop':
                        self._prepare_df_time(df, tz)
                else:
                    raise FunctionFailed(f'{req.status_code} {req.text}')
            else:
                params['i'] = list(self.oid_map)
                stats = self.client.call('item.state_history', params)
                for oid, proc in self.oid_map.items():
                    if not proc['status']:
                        del stats[f'{oid}/status']
                    if not proc['value']:
                        del stats[f'{oid}/value']
                cols = {}
                if t_col in ['keep', 'index']:
                    times = stats.pop('t')
                else:
                    del stats['t']
                    times = None
                df = pd.DataFrame(data=stats)
                if times is not None:
                    df.insert(0, 'time', times)
                for oid, proc in self.oid_map.items():
                    if not isinstance(proc['status'], bool):
                        cols[f'{oid}/status'] = proc['status']
                    if not isinstance(proc['value'], bool):
                        cols[f'{oid}/value'] = proc['value']
                if cols:
                    df = df.rename(columns=cols)
                if t_col != 'drop':
                    self._prepare_df_time(df, tz)
                return df
            if t_col == 'index':
                df = df.set_index('time')
        return df


def _history_df(self):
    return HistoryDF(self)


HttpClient.history_df = _history_df
BusRpcClient.history_df = _history_df
