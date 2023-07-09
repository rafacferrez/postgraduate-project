# Databricks notebook source
class Widget:

    @staticmethod
    def get(name, default):
        try: 
            return dbutils.widgets.get(name)
        except:
            return default
