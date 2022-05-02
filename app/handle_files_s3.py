###Lee archivos. Devuelve archivos concatenados y lista
###Borra archivos de la lista anterior!!!
from typing import Any, List


import boto3  # pylint: disable=E0401
import pandas as pd


class S3BucketClient:
    """Implements an abstraction to interact with an s3 bucket.

    Attributes
    ----------
        bucket: str
            the name of the s3 bucket to interact
        client: boto3.client
            an instantiated s3 client
    """

    def __init__(self, bucket: str) -> None:
        """Constructor method for this class.

        Parameters
        ----------
            bucket: str
                the name of the s3 bucket to interact
        """

        self.bucket = bucket
        self.client = boto3.client("s3")

    def get_files_names(self, prefix: str) -> List[str]:
        """Get all file names for a given s3 directory (prefix).

        Parameters
        ----------
            prefix: str
                the prefix (directory) where to look for files.
                For example: 'data/raw/new_client'

        Returns
        -------
            list:
                a list with paths to all found files
        Raises
        ------
            TODO
        """
        response = self.client.list_objects(
            Bucket=self.bucket,
            Marker=prefix + "/",
        )
        return [x["Key"] for x in response["Contents"]]
        # Add exception if no files found!!!
