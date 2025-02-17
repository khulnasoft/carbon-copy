"""
Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import time
import threading
from operator import itemgetter
from random import choice
from collections import defaultdict, deque

from carbon.conf import settings
from carbon import events, log
from carbon.pipeline import Processor
from carbon.util import TaggedSeries


def by_timestamp(t_v):
    """Sort key function to extract timestamp from a tuple."""
    timestamp, _ = t_v
    return timestamp


class CacheFeedingProcessor(Processor):
    """Processor that feeds data into the MetricCache."""

    plugin_name = "write"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = MetricCache.get_instance()

    def process(self, metric, datapoint):
        """Processes and normalizes metric names before storing."""
        try:
            metric = TaggedSeries.parse(metric).path
        except Exception as err:
            log.msg(f"Error parsing metric {metric}: {err}")

        self.cache.store(metric, datapoint)
        return Processor.NO_OUTPUT


class DrainStrategy:
    """Base class for strategies that determine the cache draining order."""

    def __init__(self, cache):
        self.cache = cache

    def choose_item(self):
        raise NotImplementedError()

    def store(self, metric):
        pass


class NaiveStrategy(DrainStrategy):
    """Pops points in an unordered fashion."""

    def __init__(self, cache):
        super().__init__(cache)
        self.queue = self._generate_queue()

    def _generate_queue(self):
        while True:
            metric_names = list(self.cache.keys())
            while metric_names:
                yield metric_names.pop()

    def choose_item(self):
        return next(self.queue)


class MaxStrategy(DrainStrategy):
    """Pops the metric with the greatest number of stored points."""

    def choose_item(self):
        metric_name, _ = max(self.cache.items(), key=lambda x: len(x[1]))
        return metric_name


class RandomStrategy(DrainStrategy):
    """Pops points randomly."""

    def choose_item(self):
        return choice(list(self.cache.keys()))  # nosec


class SortedStrategy(DrainStrategy):
    """Prefers metrics with more cached points but ensures all points are written once per loop."""

    def __init__(self, cache):
        super().__init__(cache)
        self.queue = self._generate_queue()

    def _generate_queue(self):
        while True:
            start_time = time.time()
            metric_counts = sorted(self.cache.counts, key=lambda x: x[1])
            size = len(metric_counts)

            if settings.LOG_CACHE_QUEUE_SORTS and size:
                log.msg(f"Sorted {size} cache queues in {time.time() - start_time:.6f} seconds")

            while metric_counts:
                yield metric_counts.pop()[0]

            if settings.LOG_CACHE_QUEUE_SORTS and size:
                log.msg(f"Queue consumed in {time.time() - start_time:.6f} seconds")

    def choose_item(self):
        return next(self.queue)


class TimeSortedStrategy(DrainStrategy):
    """Prefers metrics that are lagging behind, ensuring all points are written once per loop."""

    def __init__(self, cache):
        super().__init__(cache)
        self.queue = self._generate_queue()

    def _generate_queue(self):
        while True:
            start_time = time.time()
            metric_lw = sorted(self.cache.watermarks, key=lambda x: x[1], reverse=True)

            if settings.MIN_TIMESTAMP_LAG:
                metric_lw = [x for x in metric_lw if time.time() - x[1] > settings.MIN_TIMESTAMP_LAG]

            size = len(metric_lw)

            if settings.LOG_CACHE_QUEUE_SORTS and size:
                log.msg(f"Sorted {size} cache queues in {time.time() - start_time:.6f} seconds")

            if not metric_lw:
                yield None

            while metric_lw:
                yield metric_lw.pop()[0]

    def choose_item(self):
        return next(self.queue)


class BucketMaxStrategy(DrainStrategy):
    """Similar to 'max' strategy but sorts metrics into buckets upon insertion."""

    def __init__(self, cache):
        super().__init__(cache)
        self.buckets = []

    def choose_item(self):
        try:
            while not self.buckets[-1]:
                self.buckets.pop()
            return self.buckets[-1].pop(0)
        except (KeyError, IndexError):
            return None

    def store(self, metric):
        nr_points = len(self.cache[metric])

        while nr_points > len(self.buckets):
            self.buckets.append([])

        if nr_points > 1:
            self.buckets[nr_points - 2].remove(metric)

        self.buckets[nr_points - 1].append(metric)


class _MetricCache(defaultdict):
    """A singleton dictionary of metric names and their cached datapoints."""

    def __init__(self, strategy=None):
        super().__init__(dict)
        self.lock = threading.Lock()
        self.size = 0
        self.new_metrics = deque()
        self.strategy = strategy(self) if strategy else None

    def store(self, metric, datapoint):
        """Stores a metric's datapoint, handling duplicates and cache limits."""
        timestamp, value = datapoint
        with self.lock:
            if timestamp in self[metric]:
                self[metric][timestamp] = value
                return

            if self.is_full:
                log.msg(f"MetricCache is full: size={self.size}")
                events.cacheOverflow()
                return

            if self.is_nearly_full:
                log.msg(f"MetricCache is nearly full: size={self.size}")
                events.cacheFull()

            if not self[metric]:
                self.new_metrics.append(metric)

            self.size += 1
            self[metric][timestamp] = value

            if self.strategy:
                self.strategy.store(metric)


class MetricCache:
    """Singleton class to manage the metric cache instance."""

    _instance = None

    @staticmethod
    def get_instance():
        if MetricCache._instance is None:
            strategy_classes = {
                "naive": NaiveStrategy,
                "max": MaxStrategy,
                "sorted": SortedStrategy,
                "timesorted": TimeSortedStrategy,
                "random": RandomStrategy,
                "bucketmax": BucketMaxStrategy,
            }
            strategy_class = strategy_classes.get(settings.CACHE_WRITE_STRATEGY, DrainStrategy)
            MetricCache._instance = _MetricCache(strategy_class)
        return MetricCache._instance
