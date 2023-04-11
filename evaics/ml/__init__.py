__version__ = '0.1.5'

import sys
import requests
import gzip
import logging
import time
import json
import busrt
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.compute as pc

from datetime import datetime
from typing import Union
from io import BytesIO

from evaics.client import HttpClient
from evaics.sdk import OID, FunctionFailed, pack, unpack
from collections import OrderedDict

from busrt.rpc import Rpc as BusRpcClient

MIME_CVS = 'text/csv'
MIME_ARROW_STREAM = 'application/vnd.apache.arrow.stream'
MIME_ARROW_FILE = 'application/vnd.apache.arrow.file'


class HistoryDF:
    """
    Fetches data from EVA ICS v4 history databases

    When the primary module is imported, the method "history_df()" is
    automatically added to evaics.client.HttpClient
    """

    def __init__(self, client, params_csv: str = None):
        """
        Create HistoryDF object instance

        All configuration methods of the class can be used in chains.

        Args:
            client: HTTP client object

        Optional:
            params_csv: CSV file or stream to read parameters from
        """
        self.client = client
        self.mlkit = None
        self.params = {'fill': '1S'}
        self.oid_map = []
        if params_csv is not None:
            self.read_params_csv(params_csv)

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
        params = csv.read_csv(f).to_pylist()
        for row in params:
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
        m['oid'] = str(oid)
        self.oid_map.append(m)
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
        if isinstance(t_end, datetime):
            t_end = t_end.timestamp()
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
        for v in self.oid_map:
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
        for v in self.oid_map:
            v['database'] = database
        return self

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

    def _post(self, *args, accept_gzip=False, headers=None, **kwargs):
        if not self.client.token:
            self.client.authenticate()
            refreshed = True
        else:
            refreshed = False
        while True:
            req_headers = {
                'x-auth-key': self.client.token,
            }
            if headers is not None:
                req_headers.update(headers)
            req = requests.post(*args,
                                headers=req_headers,
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
            if isinstance(data, str):
                if data.endswith('.arrows'):
                    mime = MIME_ARROW_STREAM
                elif data.endswith('.arrow'):
                    mime = MIME_ARROW_FILE
                else:
                    mime = MIME_CVS
                files['file'] = ('', open(data, 'rb'), mime)
            elif isinstance(data, pa.Table):
                buf = BytesIO()
                with pa.ipc.new_stream(buf, data.schema) as writer:
                    writer.write(data)
                buf.seek(0)
                files['file'] = ('', buf, MIME_ARROW_STREAM)
            else:
                raise ValueError('unsupported data kind')
            req = self._post(f'{ml_url}/ml/api/upload.item.state_history',
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

    def _fetch_bus(self, t_col, strict_col_order):
        params = self._prepare_mlkit_params(t_col)
        res = self.client.call(
            self._get_mlkit(),
            busrt.rpc.Request('Citem.state_history',
                              pack(params))).wait_completed()
        cpl = res.get_payload()
        names = []
        data = []
        while True:
            res = self.client.call(self._get_mlkit(),
                                   busrt.rpc.Request('N',
                                                     cpl)).wait_completed()
            payload = res.get_payload()
            if payload:
                buf = BytesIO(payload)
                while True:
                    try:
                        f = pa.ipc.open_stream(buf)
                        names += f.schema.names
                        for b in f:
                            data += b.columns
                    except pa.lib.ArrowInvalid:
                        break
                if strict_col_order:
                    _reoder(self.oid_map, names, data, t_col=t_col)
                return pa.Table.from_arrays(data, names)
            else:
                break

    def _fetch_mlsrv_http(self, ml_url, t_col, strict_col_order):
        params = self._prepare_mlkit_params(t_col)
        req = self._post(f'{ml_url}/ml/api/query.item.state_history',
                         headers={
                             'accept': MIME_ARROW_STREAM,
                             'accept-encoding': 'gzip'
                         },
                         json=params,
                         stream=True)
        if req.ok:
            if req.headers.get('content-encoding') == 'gzip':
                stream = gzip.GzipFile(fileobj=req.raw, mode='rb')
            else:
                stream = req.raw
            names = []
            data = []
            while True:
                try:
                    f = pa.ipc.open_stream(stream)
                    names += f.schema.names
                    for b in f:
                        data += b.columns
                except pa.lib.ArrowInvalid:
                    break
            if strict_col_order:
                _reoder(self.oid_map, names, data, t_col=t_col)
            return pa.Table.from_arrays(data, names)
        else:
            raise FunctionFailed(f'{req.status_code} {req.text}')

    def _fetch_hmi_http(self, t_col):
        params = self.params.copy()
        params['i'] = [m.pop('oid') for m in self.oid_map]
        stats = self.client.call('item.state_history', params)
        data = OrderedDict()
        cols = {}
        if t_col == 'keep':
            data['time'] = stats.pop('t')
        for (oid, proc) in zip(params['i'], self.oid_map):
            if proc['status'] == True:
                col = f'{oid}/status'
                data[col] = stats.pop(col)
            elif isinstance(proc['status'], str):
                col = proc['status']
                data[col] = stats.pop(f'{oid}/status')
            if proc['value'] == True:
                col = f'{oid}/value'
                data[col] = stats.pop(col)
            elif isinstance(proc['value'], str):
                col = proc['value']
                data[col] = stats.pop(f'{oid}/value')
        return pa.Table.from_pydict(data)

    def fetch(self,
              t_col: str = 'keep',
              tz: str = 'local',
              output='arrow',
              strict_col_order=True):
        """
        Fetch data

        Optional:
            output: output format (arrow, pandas or polars)
            t_col: time column processing, "keep" - keep the column, "drop" -
            drop the time column
            tz: time zone (local, custom or None to keep time column as UNIX
            timestamp), the default is "local"
            strict_col_order: force strict column ordering (default: True)


        Returns:
            a prepared Pandas DataFrame object
        """
        if t_col not in ['keep', 'drop']:
            raise RuntimeError('unsupported t_col op')
        if output not in ['arrow', 'pandas', 'polars']:
            raise RuntimeError('unsupported output type')
        if isinstance(self.client, BusRpcClient):
            from_ipc = True
            df = self._fetch_bus(t_col, strict_col_order)
        else:
            ml_url = self._get_ml_url()
            if ml_url:
                from_ipc = True
                df = self._fetch_mlsrv_http(ml_url, t_col, strict_col_order)
            else:
                from_ipc = False
                df = self._fetch_hmi_http(t_col)
        return _finalize_data(df,
                              t_col=t_col,
                              output=output,
                              tz=tz,
                              from_ipc=from_ipc)


def _finalize_data(df, t_col=None, output=None, tz=None, from_ipc=False):
    if output == 'arrow':
        return df
    if tz == 'local':
        tz = time.tzname[0]
    if output == 'pandas':
        import pandas as pd
        df = df.to_pandas()
        if len(df) > 0 and t_col != 'drop' and tz is not None:
            if not from_ipc:
                df['time'] = pd.to_datetime(df['time'], unit='s')
            df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert(tz)
        return df
    elif output == 'polars':
        import polars as pl
        df = pl.from_arrow(df)
        if len(df) > 0 and t_col != 'drop' and tz is not None:
            if not from_ipc:
                df = df.with_column(pl.from_epoch('time', unit='s'))
            df = df.with_columns(df['time'].dt.replace_time_zone('UTC'))
            df = df.with_columns(df['time'].dt.convert_time_zone(tz))
        return df
    else:
        raise RuntimeError('output unsupported')


def _reoder(oid_map, names, data, t_col=None):
    col_names = []
    if t_col == 'keep':
        col_names.append('time')
    for m in oid_map:
        if m['status'] in ['true', '1', True]:
            col_names.append(_default_col_name(m['oid'], 'status'))
        elif m['status'] not in [None, False, 'false', '0', 'null']:
            col_names.append(str(m['status']))
        if m['value'] in ['true', '1', True]:
            col_names.append(_default_col_name(m['oid'], 'value'))
        elif m['value'] not in [None, False, 'false', '0', 'null']:
            col_names.append(str(m['value']))
    for i, col in enumerate(col_names):
        try:
            pos = names.index(col)
            if pos != i:
                _swap_entry(names, i, pos)
                _swap_entry(data, i, pos)
        except ValueError:
            pass


def _swap_entry(arr, pos1, pos2):
    entry1 = arr.pop(pos1)
    entry2 = arr.pop(pos2 - 1)
    arr.insert(pos2 - 1, entry1)
    arr.insert(pos1, entry2)


def _default_col_name(oid, prop):
    return f'{oid}/{prop}'


def _history_df(self, params_csv=None):
    return HistoryDF(self, params_csv=params_csv)


HttpClient.history_df = _history_df
BusRpcClient.history_df = _history_df
