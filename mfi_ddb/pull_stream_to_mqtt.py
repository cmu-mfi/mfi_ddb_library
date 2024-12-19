#!/usr/bin/env python3

import os
import random
import time

import xmltodict
import yaml
from mqtt_spb_wrapper import MqttSpbEntityDevice
from omegaconf import OmegaConf

from mfi_ddb import PushStreamToMqtt


class PullStreamToMqtt:
    def __init__(self):      
        assert False, "PullStreamToMqtt not implemented yet!"