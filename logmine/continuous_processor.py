import sys
import os
import multiprocessing
from .clusterer import Clusterer
from .cluster_merge import ClusterMerge
from logmine.continuous_map_reduce import ContinuousMapReduce
from .debug import log


FIXED_MAP_JOB_KEY = 1  # Single key for the whole map-reduce operation


class ContinousProcessor():
    def __init__(self, config, cluster_config):
        self.cluster_config = cluster_config
        self.config = config
        log("ContinousProcessor: process multi cores")
        # Perform clustering all chunks in parallel
        self.mapper = MapReduce(
            map_segments_to_clusters,
            reduce_clusters,
            params=self.cluster_config
        )


    def process(self, inputs):
        """
        Process inputs in parallel with multiple processes.

        This is a little bit different than the approach described in the
        LogMine paper. Each "map job" is a chunk of multiple lines (instead of
        a single line), this helps utilizing multiprocessing better.

        Do note that this method may return different result in each run, and
        different with the other version "process_single_core". This is
        expected, as the result depends on the processing order - which is
        not guaranteed when tasks are performed in parallel.
        """

        self.mapper.map(inputs)

    def complete(self):
        results = self.mapper.reduce() 
        log("Processor: result", result)

        if len(result) == 0:
            return []

        (key, clusters) = result[0]
        return clusters



# The methods below are used by multiprocessing.Pool and need to be defined at
# top level

def map_lines_to_clusters(x):
    log('mapper: %s working on %s' % (os.getpid(), x))
    (lines, config) = x
    clusterer = Clusterer(**config)
    clusters = clusterer.find(lines)
    return [(FIXED_MAP_JOB_KEY, clusters)]


def reduce_clusters(x):
    """
    Because all map job have the same key, this reduce operation will be
    executed in one single processor. Most of the time, the number of clusters
    in this step is small so it is kind of acceptable.
    """
    log('reducer: %s working on %s items' % (os.getpid(), len(x[0][1])))
    # a = [debug_print(i) for i in x[0][1]]
    ((key, clusters_groups), config) = x
    if len(clusters_groups) <= 1:
        return (key, clusters_groups)  # Nothing to merge

    base_clusters = clusters_groups[0]
    merger = ClusterMerge(config)
    for clusters in clusters_groups[1:]:
        # print('merging %s-%s' % (len(base_clusters), len(clusters)))
        merger.merge(base_clusters, clusters)
    return (key, base_clusters)
