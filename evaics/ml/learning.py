import pandas as pd
import numpy as np
import pickle

from types import SimpleNamespace
from tqdm import tqdm


def verify_array(arr: np.array, verbose=1, description=''):
    data = arr.ravel()
    rng = range(0, len(data))
    if verbose:
        rng = tqdm(rng)
        rng.set_description(description)
    for i in rng:
        if abs(data[i]) > 1:
            raise ValueError(
                'scaled data deviation is out of the recommended range')


def df_to_rolling_windows(df: pd.DataFrame,
                          window_size: int,
                          shift: int = 0,
                          limit: int = None,
                          verbose=1,
                          description=''):
    """
    Convert data frame to rolling windows numpy array

    Args:
        window_size: desired window size

    Optional:
        shift: shift data N rows ahead
        limit: limit the result data to N windows
        verbose: show data processing progress bar
        description: description for the progress bar if enabled

    Returns:
        processed numpy array
    """
    data = []
    n = len(df) - window_size - shift
    if limit is not None:
        l = limit + shift
        if l < n:
            n = l
    rng = range(shift, n)
    if verbose:
        rng = tqdm(rng)
        rng.set_description(description)
    for i in rng:
        data.append(df[i:i + window_size].to_numpy())
    return np.array(data)


def save_tf_session(path, model, data):
    """
    Saves model and session data

    Args:
        path: file path (without extension)
        model: TensorFlow model
        data: any object
    """
    model.save(f'{path}.h5')
    with open(f'{path}.dat', 'wb') as fh:
        pickle.dump(data, fh)


def load_tf_session(path):
    """
    Loads model and session data

    Args:
        path: file path (without extension)

    Returns:
        (model, data) tuple
    """
    from tensorflow import keras
    model = keras.models.load_model(f'{path}.h5')
    with open(f'{path}.dat', 'rb') as fh:
        data = pickle.load(fh)
    return (model, data)


class Regression:
    """
    A helper class for standard regression analysis

    All class configuration and load/save functions can be chained
    """

    def __init__(self, window_size: int = 30):
        """
        Optional:
            window_size: desired window size (for new models), default is 30
        """
        self._params = SimpleNamespace(window_size=window_size,
                                       scaled=False,
                                       scaler=None,
                                       y_cols=None,
                                       y_shift=None)
        self.dataframes = SimpleNamespace(x=None, y=None)
        self.data = SimpleNamespace(x=None, y=None)
        self.model = None

    def get_window_size(self):
        """
        Get window size

        Returns:
            window size
        """
        return self._params.window_size

    def load(self, path):
        """
        Loads model and session data

        Args:
            path: file path (without extension)
        """
        (self.model, self._params) = load_tf_session(path)
        return self

    def save(self, path):
        """
        Saves model and session data

        Args:
            path: file path (without extension)
        """
        self._require('model')
        save_tf_session(path, self.model, self._params)
        return self

    def with_scaler(self, scaler):
        """
        Set a custom scaler

        Must have the same API as MinMaxScaler

        Args:
            scaler: custom scaler object
        """
        self._params.scaler = scaler
        return self

    def with_standard_scaler(self):
        """
        Set scaler to MinMaxScaler
        """
        from sklearn.preprocessing import MinMaxScaler
        self._params.scaler = MinMaxScaler()
        return self

    def with_training_data(self,
                           data: pd.DataFrame,
                           y_cols: list = None,
                           y_shift: int = None):
        """
        Set training data

        Args:
            data: pandas.DataFrame

        Note: for the moment the supplied regression model supports a single
        Y-col only

        Optional:
            y_cols: a Y-column which is used as the training target, required
                    for the first training data set, can not be set later
            y_shift: shift Y-column N rows ahead, ignored for opaquie trainings
                    (the first set value is used)
        """
        self._require('scaler')
        if self._params.y_cols is None:
            if y_cols is None:
                raise RuntimeError('y_cols required for the first data set')
            self._params.y_cols = y_cols if isinstance(y_cols,
                                                       list) else [y_cols]
            if len(self._params.y_cols) != 1:
                raise ValueError('y_cols must have 1 column')
        elif self._params.y_cols is not None and y_cols is not None:
            raise RuntimeError('data model y_cols already set')
        if self._params.y_shift is None:
            self._params.y_shift = self._params.window_size if y_shift is None \
                    else y_shift
        if self._params.scaled:
            df = pd.DataFrame(self._params.scaler.transform(data),
                              columns=data.columns)
        else:
            df = pd.DataFrame(self._params.scaler.fit_transform(data),
                              columns=data.columns)
            self._params.scaled = True
        ydata = []
        for col in self._params.y_cols:
            ydata.append(pd.DataFrame(df.pop(col), columns=[col]))
        self.dataframes.x = df
        self.dataframes.y = pd.concat(ydata, axis=1)
        return self

    def with_prediction_data(self, data: pd.DataFrame):
        """
        Set prediction data

        The function automatically removes Y-colums from the data if found,
        send data.copy() if the data is required later

        Args:
            data: pandas.DataFrame
        """
        self._require('scaler')
        self._require('y_cols')
        for col in self._params.y_cols:
            data[col] = 0
        df = pd.DataFrame(self._params.scaler.transform(data),
                          columns=data.columns)
        for col in self._params.y_cols:
            del data[col]
            del df[col]
        self.dataframes.x = df
        self.dataframes.y = None
        return self

    def with_model(self, model):
        """
        Set a custom model

        The model must be compiled manually

        Args:
            model: TensorFlow model
        """
        self.model = model
        return self

    def with_standard_model(self,
                            dense_units=10,
                            loss='mse',
                            optimizer='adam',
                            **kwargs):
        """
        Set the standard model

        The standard model is a Sequential model, which contains:

        * two LSTM layers with number of units = window_size
        * N Dense units
        * one or several exit Dense units, depending on Y-cols set

        Opitonal:
            dense_units: dense units to set (default: 10)
            loss: loss function to compile the model (default: mse)
            optimizer: model optimizer (default: adam)
            kwargs: sent to model.compile as-is
        """
        self._require('df.x')
        self._require('df.y')
        from tensorflow.keras.models import Sequential
        from tensorflow.keras.layers import LSTM, Dense
        self.model = Sequential([
            LSTM(self._params.window_size,
                 input_shape=(self._params.window_size,
                              len(self.dataframes.x.columns)),
                 return_sequences=True),
            LSTM(self._params.window_size, return_sequences=False),
            Dense(dense_units * len(self.dataframes.y.columns)),
            Dense(len(self.dataframes.y.columns)),
        ])
        self.model.compile(loss=loss, optimizer=optimizer, **kwargs)
        return self

    def prepare_data(self, verbose=1):
        """
        Prepares the previously set data

        Optional:
            verbose: show verbose progress (default: 1)
        """
        self._require('df.x')
        if self.dataframes.y is None:
            self.data.y = None
        else:
            self.data.y = df_to_rolling_windows(
                self.dataframes.y,
                window_size=self._params.window_size,
                shift=self._params.y_shift,
                verbose=verbose,
                description='preparing y')
        self.data.x = df_to_rolling_windows(
            self.dataframes.x,
            window_size=self._params.window_size,
            limit=None if self.data.y is None else len(self.data.y),
            verbose=verbose,
            description='preparing x')
        return self

    def verify_prepared(self, verbose=1):
        """
        Verifies prepared data

        Optional:
            verbose: verbose

        Raises:
            ValueError: if scaled data deviation is out of the recommended range
        """
        self._require('data.x')
        if self.data.y is not None:
            verify_array(self.data.y,
                         verbose=verbose,
                         description='verifying y')
        verify_array(self.data.x, verbose=verbose, description='verifying x')
        return self

    def fit_model(self, epochs, shuffle=False, verbose=1, **kwargs):
        """
        Trains the model

        The data must be set and prepared

        Args:
            epochs: epochs to train

        Optional:
            shuffle: default: False
            verbose: default: 1
            kwargs: sent to model.fit as-is
        """
        self._require('data.x')
        self._require('data.y')
        self._require('model')
        return self.model.fit(self.data.x,
                              self.data.y,
                              epochs=epochs,
                              shuffle=shuffle,
                              verbose=verbose,
                              **kwargs)

    def predict(self, col_suffix='_predict', verbose=1, **kwargs):
        """
        Perform prediction using the pre-trained model

        The data must be set and prepared

        Optional:
            col_suffix: append a suffix to Y-cols, e.g. temp: temp_SFX, the
                        default is "_predict". Set to none to keep columns as
                        they are
            verbose: default: 1
        """
        self._require('data.x')
        self._require('model')
        self._require('y_cols')
        pred = self.model.predict(self.data.x, verbose=verbose, **kwargs)
        data = pd.concat([
            self.dataframes.x[:-self._params.window_size],
            pd.DataFrame(pred, columns=self._params.y_cols)
        ],
                         axis=1)
        df = pd.DataFrame(self._params.scaler.inverse_transform(data),
                          columns=data.columns)
        for col in data.columns:
            if col not in self._params.y_cols:
                del df[col]
        if col_suffix is not None:
            df.rename(columns={
                col: f'{col}{col_suffix}' for col in self._params.y_cols
            },
                      inplace=True)
        return df

    def _require(self, what):
        if what == 'scaler':
            if self._params.scaler is None:
                raise RuntimeError('scaler not set')
        elif what == 'model':
            if self.model is None:
                raise RuntimeError('model not set')
        elif what == 'df.x':
            if self.dataframes.x is None:
                raise RuntimeError('data not set (x)')
        elif what == 'df.y':
            if self.dataframes.y is None:
                raise RuntimeError('data not set (y)')
        elif what == 'data.x':
            if self.data.x is None:
                raise RuntimeError('data not prepared (x)')
        elif what == 'data.y':
            if self.dataframes.y is None:
                raise RuntimeError('data not prepared (y)')
        elif what == 'y_cols':
            if self._params.y_cols is None:
                raise RuntimeError('training data has been never set')
        else:
            raise RuntimeError(f'internal error, invalid attr {what}')
