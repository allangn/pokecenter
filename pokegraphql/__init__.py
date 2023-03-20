import requests, hashlib
import pandas as pd
from ratelimit import limits
from .exceptions import queryError

class PokeGraphql:
    """
    A class for querying the PokeAPI using GraphQL.

    Args:
        query (str): A string representing the GraphQL query.

    Attributes:
        resp (requests.models.Response): The response object of the POST request to the API.
        hash (str): The SHA256 hash of the response text.
        _data (dict): The parsed JSON data received from the API.
        _df (pandas.DataFrame): The data returned by the API as a pandas DataFrame.

    Methods:
        df() -> pd.DataFrame: Returns the pandas DataFrame object containing the API data.
        expand_nested(): Expands nested JSON data in the DataFrame by creating new columns for each key.

    Raises:
        queryError: An exception raised when the API returns an error status code.
    """

    @limits(calls=100, period=3600)
    def __init__(self, query: str):
        """
        Initializes a PokeGraphql object.

        Args:
            query (str): A string representing the GraphQL query.
        """
        headers = {
            "content-type": "application/json",
            "accept": "*/*",
            "Accept-Encoding": "gzip"
            #Accept-Encoding: gzip does not seem to be implemented for this API: https://graphql.org/learn/best-practices/#json-with-gzip
        }
        
        self.resp = requests.post("https://beta.pokeapi.co/graphql/v1beta", json={"query": query}, headers=headers)
        self._data = self.resp.json()
        self.hash = hashlib.sha256(self.resp.text.encode("utf-8")).hexdigest()

        #handle that graphql returns status 200 on errors
        if "errors" in self._data.keys():
            raise  queryError(f"{self._data['errors'][0]['message']}, code: {self._data['errors'][0]['extensions']['code']}")

        self._df = pd.DataFrame(self._data["data"])

    @property
    def df(self) -> pd.DataFrame:
        """
        Returns the pandas DataFrame object containing the API data.

        Returns:
            pd.DataFrame: The DataFrame object containing the API data.
        """
        return self._df
   
    def expand_nested(self):
        """
        Expands nested JSON data in the DataFrame by creating new columns for each key.
        """
        is_expandable = True
        while is_expandable: 
            types = [column for column in self._df.columns if isinstance(self._df[column][0], dict) or isinstance(self._df[column][0], list)]

            if types == []:
                is_expandable = False

            else: 
                for column in types:
                    expanded = pd.json_normalize(self._df[column])
                    expanded = expanded.add_prefix(f"{column}_")
                    self._df = pd.concat([self._df, expanded], axis=1)
                    self._df.drop(column, axis=1, inplace=True)