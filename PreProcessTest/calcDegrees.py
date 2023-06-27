import pandas as pd
import numpy as np
import os
import math
import gc
import copy


class DegreeStatistics:
    def __init__(self):
        self.fullGeoFile: str = os.path.join("..", r"raw_data\PeMS07\PeMS07.geo")
        self.fullRelFile: str = os.path.join("..", r"raw_data\PeMS07\PeMS07.rel")
        self.outPut: str = os.path.join("..", r"raw_data\PeMS07\PeMS07_DegStat.csv")
        self.fullGeo: pd.DataFrame = pd.read_csv(self.fullGeoFile, index_col="geo_id")
        self.fullRel: pd.DataFrame = pd.read_csv(self.fullRelFile, index_col="rel_id")
        # 入度
        self.posDegMap = dict(zip(self.fullGeo.index, [0] * len(self.fullGeo.index)))
        # 出度
        self.negDegMap = dict(zip(self.fullGeo.index, [0] * len(self.fullGeo.index)))
        self.degMap = dict(zip(self.fullGeo.index, [0] * len(self.fullGeo.index)))
        self.nodeDegs = None
        self.roadStatusStat = {}
        self.calc()

    def calc(self):
        for src, dst in zip(self.fullRel.origin_id, self.fullRel.destination_id):
            self.posDegMap[src] += 1
            self.negDegMap[dst] += 1
            self.degMap[src] += 1
            self.degMap[dst] += 1
        self.nodeDegs = pd.concat([pd.DataFrame([self.degMap]), pd.DataFrame([self.posDegMap]),
                                   pd.DataFrame([self.negDegMap])], sort=False).T
        self.nodeDegs.columns = ['deg', 'pos_deg', 'neg_deg']
        self.nodeDegs.to_csv(self.outPut)
        for src, dst in zip(self.fullRel.origin_id, self.fullRel.destination_id):
            __w = self.nodeDegs.deg[src] + self.nodeDegs.deg[dst]
            try:
                self.roadStatusStat[__w] += 1
            except KeyError:
                self.roadStatusStat[__w] = 1
        print(self.roadStatusStat)


if __name__ == "__main__":
    DegreeStatistics()
