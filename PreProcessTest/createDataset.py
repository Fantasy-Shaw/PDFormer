import random

import pandas as pd
import numpy as np
import os
import math
import gc
import copy


class IncrementalDataGenerator():
    def __init__(self):
        self.fullDynaFile: str = os.path.join("..", r"raw_data\PeMS07\PeMS07.dyna")
        self.fullGeoFile: str = os.path.join("..", r"raw_data\PeMS07\PeMS07.geo")
        self.fullRelFile: str = os.path.join("..", r"raw_data\PeMS07\PeMS07.rel")
        self.outPutDir: str = os.path.join("..", r"raw_data\PeMS07LiteTest")
        self.fullDyna: pd.DataFrame = pd.read_csv(self.fullDynaFile, index_col="dyna_id")
        self.fullGeo: pd.DataFrame = pd.read_csv(self.fullGeoFile, index_col="geo_id")
        self.fullRel: pd.DataFrame = pd.read_csv(self.fullRelFile, index_col="rel_id")
        self.startSensorIdx = 571
        self.endSensorIdx = np.inf
        self.rdShuffleLength = 320
        # self.genNumOrderDataSet()
        self.genRandomShuffledDataset()

    def genNumOrderDataSet(self):
        if not os.path.exists(self.outPutDir):
            os.makedirs(self.outPutDir)
        dyna = copy.deepcopy(self.fullDyna)
        dyna.drop(dyna[(dyna.entity_id < self.startSensorIdx) | (dyna.entity_id > self.endSensorIdx)].index,
                  inplace=True)
        dyna = dyna.reset_index()
        dyna["dyna_id"] = dyna.index
        dyna.to_csv(os.path.join(self.outPutDir, "dyna.csv"), index=False)
        geo = copy.deepcopy(self.fullGeo)
        # Should be as follows, while geo["geo_id"] as geo.index.
        geo.drop(geo[(geo.index < self.startSensorIdx) | (geo.index > self.endSensorIdx)].index, inplace=True)
        geo.to_csv(os.path.join(self.outPutDir, "geo.csv"), index=True)
        rel = copy.deepcopy(self.fullRel)
        rel.drop(rel[(rel.origin_id < self.startSensorIdx) | (rel.origin_id > self.endSensorIdx) |
                     (rel.destination_id < self.startSensorIdx) | (rel.destination_id > self.endSensorIdx)].index,
                 inplace=True)
        rel = rel.reset_index()
        rel["rel_id"] = rel.index
        rel.to_csv(os.path.join(self.outPutDir, "rel.csv"), index=False)
        del dyna, geo, rel
        gc.collect()

    def genRandomShuffledDataset(self):
        if not os.path.exists(self.outPutDir):
            os.makedirs(self.outPutDir)
        baseIndices = list(set(random.sample(range(len(self.fullGeo)), self.rdShuffleLength)))
        print(len(baseIndices), baseIndices)
        dyna = copy.deepcopy(self.fullDyna)
        dyna.drop([i for i, x in enumerate(dyna.entity_id) if x not in baseIndices], inplace=True)
        dyna = dyna.reset_index()
        dyna["dyna_id"] = dyna.index
        dyna.to_csv(os.path.join(self.outPutDir, "dyna.csv"), index=False)
        geo = copy.deepcopy(self.fullGeo)
        # Should be as follows, while geo["geo_id"] as geo.index.
        geo.drop([i for i, x in enumerate(geo.index) if x not in baseIndices], inplace=True)
        geo.to_csv(os.path.join(self.outPutDir, "geo.csv"), index=True)
        rel = copy.deepcopy(self.fullRel)
        rel.drop([i for i, x in enumerate(zip(rel.origin_id, rel.destination_id)) if
                  (x[0] not in baseIndices) or (x[1] not in baseIndices)], inplace=True)
        rel = rel.reset_index()
        rel["rel_id"] = rel.index
        rel.to_csv(os.path.join(self.outPutDir, "rel.csv"), index=False)
        del dyna, geo, rel
        gc.collect()


if __name__ == "__main__":
    IncrementalDataGenerator()
