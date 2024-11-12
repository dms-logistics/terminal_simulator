import json
import os
import numpy as np
import pandas as pd
import logging
import datetime
import time
from pymongo import MongoClient


class DataBase:

    # connect to MongoDB
    @staticmethod
    def getMongoConnection(DB: str, string_conncetion: str):
        """_summary_

        Args:
            DB (str): _description_

        Returns:
            _type_: _description_
        """

        mongoStringConnection = os.environ.get(string_conncetion)
        # print(f"DEBUG: getMongoConnection : {mongoStringConnection}")
        client = MongoClient(mongoStringConnection)

        mongoDatabase = client[DB]

        return mongoDatabase

    def pushDataFrameIntoMongoCollection(self, dbName: str, collectionName: str, stringConncetion: str, df: pd.DataFrame,
                                         deleteExistingDocumentsBeforePush: bool = True, deleteQuery={}) -> None:
        """Push dataframe into a mongo collection  

        Args:
            dbName (str): database name
            collectionName (str): collection name 
            stringConncetion (str): string connection field name as declared in .env (ex: SEAYARD_MONGO_STAGING_CONN)
            df (pd.DataFrame): dataframe with well formated datetime columns (ex: NaN values)
            deleteExistingDocumentsBeforePush (bool, optional): boolean confirming whether or not to delete existing 
            documents in collection, if collection already exists in DB. Defaults to True.
            deleteQuery (dict, optional): custom delete query if needed. Defaults to {}.
        """
        start_time = time.time()
        logging.info("Convert DataFrame to dict ...")
        df_Rec = df.to_dict('records')
        logging.info("- "*30)
        logging.info(df_Rec[:1])
        logging.info("- "*30)
        logging.info("Conversion have been done --- %s seconds ---" %
                     (time.time() - start_time))
        DbConnection = self.getMongoConnection(dbName, stringConncetion)
        if deleteExistingDocumentsBeforePush:
            logging.info(
                f"Deleting existing documents from {collectionName} ...")
            start_time = time.time()
            try:
                DbConnection[collectionName].delete_many(deleteQuery)
            except Exception as e:
                logging.error(
                    f"Deleting existing documents from {collectionName} failed: {e}")
            logging.info(f"Deleting existing documents from {collectionName} finished --- %s seconds ---" %
                         (time.time() - start_time))
        logging.info("Inserting the new documents ...")
        start_time = time.time()
        DbConnection[collectionName].insert_many(df_Rec)
        logging.info(
            f"Number of inserted documents in {collectionName}: {len(df_Rec)}")
        logging.info("The new documents insertion have been finished --- %s seconds ---" %
                     (time.time() - start_time))

    def loadCollectionFromMongo(self, dbName: str, collectionName: str, stringConncetion: str, query: dict = None, projection=None) -> pd.DataFrame:
        """Load mongo collection as dataframe using find method

        Args:
            dbName (str): database name 
            collectionName (str): collection name 
            stringConncetion (str): string connection field name as declared in .env (ex: SEAYARD_MONGO_STAGING_CONN)
            query (dict, optional): query condition using to select documents. Defaults to None.

        Returns:
            pd.DataFrame: collection as a dataframe
        """
        start_time = time.time()
        logging.info(f"Loading {collectionName} Data ... ")
        dbConn = self.getMongoConnection(dbName, stringConncetion)
        if query is None:
            query = {}
        if projection is None:
            df_Mongo = dbConn[collectionName].find(
                query
            )
        else:
            df_Mongo = dbConn[collectionName].find(
                query, projection
            )
        df = pd.DataFrame(list(df_Mongo))
        logging.info(
            f"{collectionName} Data Loaded: --- {(time.time() - start_time):.4f} seconds ---")
        return df


class MyEncoder(json.JSONEncoder):
    """data serializer encoder for json.dumps when having the error: 
        TypeError: Object of type xxxx is not JSON serializable
    """

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, datetime.datetime):
            return str(obj)
        elif isinstance(obj, datetime.date):
            return str(obj)
        else:
            return super(MyEncoder, self).default(obj)
