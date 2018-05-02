# -*- coding: utf-8 -*-


def _init():
    global Snapshot, DataSource, factory, querymaxcodes
    Snapshot = {}
    DataSource = ''
    factory = object()
    querymaxcodes = 0
