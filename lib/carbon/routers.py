from carbon.hashing import ConsistentHashRing, carbonHash
from carbon.util import PluginRegistrar
from six import with_metaclass
from six.moves import xrange


class DatapointRouter(with_metaclass(PluginRegistrar, object)):
    """Abstract base class for datapoint routing logic implementations."""
    
    plugins = {}

    def addDestination(self, destination):
        """Adds a destination (host, port, instance) tuple."""
        raise NotImplementedError()

    def removeDestination(self, destination):
        """Removes a destination (host, port, instance) tuple."""
        raise NotImplementedError()

    def hasDestination(self, destination):
        """Checks if a destination exists in the router."""
        raise NotImplementedError()

    def countDestinations(self):
        """Returns the number of configured destinations."""
        raise NotImplementedError()

    def getDestinations(self, key):
        """Yields destinations mapped to the given key."""
        raise NotImplementedError()


class ConstantRouter(DatapointRouter):
    plugin_name = 'constant'

    def __init__(self, settings):
        self.destinations = set()

    def addDestination(self, destination):
        self.destinations.add(destination)

    def removeDestination(self, destination):
        self.destinations.discard(destination)

    def hasDestination(self, destination):
        return destination in self.destinations

    def countDestinations(self):
        return len(self.destinations)

    def getDestinations(self, key):
        yield from self.destinations


class RelayRulesRouter(DatapointRouter):
    plugin_name = 'rules'

    def __init__(self, settings):
        from carbon.relayrules import loadRelayRules

        self.rules_path = settings["relay-rules"]
        self.rules = loadRelayRules(self.rules_path)
        self.destinations = set()

    def addDestination(self, destination):
        self.destinations.add(destination)

    def removeDestination(self, destination):
        self.destinations.discard(destination)

    def hasDestination(self, destination):
        return destination in self.destinations

    def countDestinations(self):
        return len(self.destinations)

    def getDestinations(self, key):
        for rule in self.rules:
            if rule.matches(key):
                yield from (dest for dest in rule.destinations if dest in self.destinations)
                if not rule.continue_matching:
                    return


class ConsistentHashingRouter(DatapointRouter):
    plugin_name = 'consistent-hashing'

    def __init__(self, settings):
        self.replication_factor = int(settings.REPLICATION_FACTOR)
        self.diverse_replicas = settings.DIVERSE_REPLICAS
        self.instance_ports = {}  # {(server, instance): port}
        hash_type = settings.ROUTER_HASH_TYPE or 'carbon_ch'
        self.ring = ConsistentHashRing([], hash_type=hash_type)

    def addDestination(self, destination):
        server, port, instance = destination
        if (server, instance) in self.instance_ports:
            raise ValueError(f"Destination ({server}, {instance}) already configured.")
        self.instance_ports[(server, instance)] = port
        self.ring.add_node((server, instance))

    def removeDestination(self, destination):
        server, _, instance = destination
        if (server, instance) not in self.instance_ports:
            raise ValueError(f"Destination ({server}, {instance}) not configured.")
        del self.instance_ports[(server, instance)]
        self.ring.remove_node((server, instance))

    def hasDestination(self, destination):
        server, _, instance = destination
        return (server, instance) in self.instance_ports

    def countDestinations(self):
        return len(self.instance_ports)

    def getDestinations(self, metric):
        key = self.getKey(metric)
        used_servers = set()
        
        for server, instance in self.ring.get_nodes(key):
            if self.diverse_replicas and server in used_servers:
                continue
            used_servers.add(server)
            yield (server, self.instance_ports[(server, instance)], instance)

            if len(used_servers) >= self.replication_factor:
                return

    def getKey(self, metric):
        return metric


class AggregatedConsistentHashingRouter(DatapointRouter):
    plugin_name = 'aggregated-consistent-hashing'

    def __init__(self, settings):
        from carbon.aggregator.rules import RuleManager

        aggregation_rules_path = settings.get("aggregation-rules")
        if aggregation_rules_path:
            RuleManager.read_from(aggregation_rules_path)

        self.hash_router = ConsistentHashingRouter(settings)
        self.agg_rules_manager = RuleManager

    def addDestination(self, destination):
        self.hash_router.addDestination(destination)

    def removeDestination(self, destination):
        self.hash_router.removeDestination(destination)

    def hasDestination(self, destination):
        return self.hash_router.hasDestination(destination)

    def countDestinations(self):
        return self.hash_router.countDestinations()

    def getDestinations(self, key):
        resolved_metrics = [
            rule.get_aggregate_metric(key) for rule in self.agg_rules_manager.rules if rule.get_aggregate_metric(key)
        ]

        if not resolved_metrics:
            resolved_metrics.append(key)

        destinations = {dest for metric in resolved_metrics for dest in self.hash_router.getDestinations(metric)}
        yield from destinations


class FastHashRing:
    """Optimized hash ring prioritizing fast lookups over minimal rebalance."""

    def __init__(self, settings):
        self.nodes = set()
        self.sorted_nodes = []
        self.hash_type = settings.ROUTER_HASH_TYPE or 'mmh3_ch'

    def _hash(self, key):
        return carbonHash(key, self.hash_type)

    def _update_nodes(self):
        self.sorted_nodes = sorted(((self._hash(str(n)), n) for n in self.nodes), key=lambda v: v[0])

    def add_node(self, node):
        self.nodes.add(node)
        self._update_nodes()

    def remove_node(self, node):
        self.nodes.discard(node)
        self._update_nodes()

    def get_nodes(self, key):
        if not self.nodes:
            return
        seed = self._hash(key) % len(self.nodes)
        for i in xrange(seed, seed + len(self.nodes)):
            yield self.sorted_nodes[i % len(self.sorted_nodes)][1]


class FastHashingRouter(ConsistentHashingRouter):
    """Consistent Hashing Router optimized with FastHashRing."""
    
    plugin_name = 'fast-hashing'

    def __init__(self, settings):
        super().__init__(settings)
        self.ring = FastHashRing(settings)


class FastAggregatedHashingRouter(AggregatedConsistentHashingRouter):
    """Aggregated Consistent Hashing Router optimized with FastHashRing."""
    
    plugin_name = 'fast-aggregated-hashing'

    def __init__(self, settings):
        super().__init__(settings)
        self.hash_router.ring = FastHashRing(settings)
