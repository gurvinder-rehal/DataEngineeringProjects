import configparser
from pyspark import SparkConf


# loading the application configs in python dictionary

def get_app_config(env):
    config = configparser.ConfigParser()
    config.read("configs/application.INI")
    app_conf = {}
    for key, val in config.items(env):
        app_conf[key] = val
    return app_conf


# loading the pyspark configs and creating a spark conf object

def get_pyspark_config(env):
    config = configparser.ConfigParser()
    config.read("config/pyspark.INI")
    pyspark_conf = SparkConf()
    print(config.items(env))
    for key, value in config.items(env):
        pyspark_conf.set(key, value)
    return pyspark_conf

