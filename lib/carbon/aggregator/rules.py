import re
from math import floor, ceil
from os.path import exists, getmtime
from twisted.internet.task import LoopingCall
from cachetools import TTLCache, LRUCache
from carbon import log
from carbon.conf import settings
from carbon.aggregator.buffers import BufferManager


def get_cache():
    """Initialize the cache based on settings."""
    ttl = settings.CACHE_METRIC_NAMES_TTL
    size = settings.CACHE_METRIC_NAMES_MAX
    if ttl > 0 and size > 0:
        return TTLCache(size, ttl)
    return LRUCache(size) if size > 0 else {}


class RuleManager:
    def __init__(self):
        self.rules = []
        self.rules_file = None
        self.read_task = LoopingCall(self.read_rules)
        self.rules_last_read = 0.0

    def clear(self):
        self.rules.clear()

    def read_from(self, rules_file):
        """Load rules from a file and start periodic reloading."""
        self.rules_file = rules_file
        self.read_rules()
        self.read_task.start(10, now=False)

    def read_rules(self):
        """Read and parse aggregation rules if the file has changed."""
        if not exists(self.rules_file):
            self.clear()
            return

        try:
            mtime = getmtime(self.rules_file)
        except OSError as e:
            log.err(f"Failed to get mtime of {self.rules_file}: {e}")
            return

        if mtime <= self.rules_last_read:
            return

        log.aggregator(f"Reading new aggregation rules from {self.rules_file}")

        new_rules = []
        try:
            with open(self.rules_file, "r") as file:
                for line in file:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    new_rules.append(self.parse_definition(line))
        except Exception as e:
            log.err(f"Error reading rules from {self.rules_file}: {e}")
            return

        log.aggregator("Clearing aggregation buffers")
        BufferManager.clear()
        self.rules = new_rules
        self.rules_last_read = mtime

    def parse_definition(self, line):
        """Parse an aggregation rule from a line."""
        try:
            left_side, right_side = map(str.strip, line.split("=", 1))
            output_pattern, frequency = left_side.split()
            method, input_pattern = right_side.split()
            frequency = int(frequency.strip("()"))
            return AggregationRule(input_pattern, output_pattern, method, frequency)
        except ValueError:
            log.err(f"Failed to parse rule in {self.rules_file}, line: {line}")
            raise


class AggregationRule:
    def __init__(self, input_pattern, output_pattern, method, frequency):
        self.input_pattern = input_pattern
        self.output_pattern = output_pattern
        self.method = method
        self.frequency = frequency

        if method not in AGGREGATION_METHODS:
            raise ValueError(f"Invalid aggregation method '{method}'")

        self.aggregation_func = AGGREGATION_METHODS[method]
        self.regex = self.build_regex()
        self.output_template = self.build_template()
        self.cache = get_cache()

    def get_aggregate_metric(self, metric_path):
        """Return the aggregated metric name from cache or compute it."""
        if metric_path in self.cache:
            try:
                return self.cache[metric_path]
            except KeyError:
                pass  # Handle potential cache expiration

        match = self.regex.match(metric_path)
        if not match:
            return None

        extracted_fields = match.groupdict()
        try:
            result = self.output_template % extracted_fields
        except TypeError:
            log.err(f"Failed to interpolate template {self.output_template} with fields {extracted_fields}")
            result = None

        self.cache[metric_path] = result
        return result

    def build_regex(self):
        """Compile regex pattern for input metric."""
        pattern = []
        for part in self.input_pattern.split("."):
            if "<<" in part and ">>" in part:
                pre, field, post = re.split(r"<<|>>", part)
                pattern.append(f"{pre}(?P<{field}>.+?){post}")
            elif "<" in part and ">" in part:
                pre, field, post = re.split(r"<|>", part)
                pattern.append(f"{pre}(?P<{field}>[^.]+){post}")
            elif part == "*":
                pattern.append("[^.]+")
            else:
                pattern.append(part.replace("*", "[^.]*"))
        
        return re.compile(r"\.".join(pattern) + "$")

    def build_template(self):
        """Build a template for output metric naming."""
        return self.output_pattern.replace("<", "%(").replace(">", ")s")


def avg(values):
    """Compute the average of values."""
    return sum(values) / len(values) if values else None


def count(values):
    """Count the number of values."""
    return len(values) if values else None


def percentile(factor):
    """Return a function that computes the nth percentile."""
    def func(values):
        if not values:
            return None
        values.sort()
        rank = factor * (len(values) - 1)
        left = int(floor(rank))
        right = int(ceil(rank))
        return values[left] if left == right else values[left] * (right - rank) + values[right] * (rank - left)
    return func


AGGREGATION_METHODS = {
    "sum": sum,
    "avg": avg,
    "min": min,
    "max": max,
    "p50": percentile(0.50),
    "p75": percentile(0.75),
    "p80": percentile(0.80),
    "p90": percentile(0.90),
    "p95": percentile(0.95),
    "p99": percentile(0.99),
    "p999": percentile(0.999),
    "count": count,
}

# Singleton instance
RuleManager = RuleManager()

# Initialize cache with default aggregation cache size
LRUCache(settings.get("AGGREGATION_CACHE_SIZE", 10000))
