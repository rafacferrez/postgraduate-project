# Databricks notebook source
from abc import ABC, abstractmethod

class Connector(ABC):

    def __init__(self, db, entity):
        self.db = db
        self.entity = entity

    @abstractmethod
    def set_source(self):
        pass

    def load(self):
        self.set_source()
        return self.df.load()
