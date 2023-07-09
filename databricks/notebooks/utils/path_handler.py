# Databricks notebook source
import os
from datetime import date

class PathHandler:

    STORAGE_ACCOUNT = dbutils.secrets.get("adls", "storage-account")

    def __init__(self, layer, db, entity):
        self.layer = layer
        self.db = db
        self.entity = entity

    @property
    def base_path(self):
        return f"abfss://{self.layer}@{self.STORAGE_ACCOUNT}.dfs.core.windows.net/"

    
    def get_path(self, use_date=None):
        path = os.path.join(self.base_path, 
            self.db, self.entity)
        if use_date:
            dt = date.today()
            path = os.path.join(path, dt.strftime("%Y"),
                dt.strftime("%Y%m"), dt.strftime("%Y%m%d"))
        return path
