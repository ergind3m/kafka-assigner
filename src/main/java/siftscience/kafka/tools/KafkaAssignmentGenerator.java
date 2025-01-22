package siftscience.kafka.tools;

import java.util.*;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.cluster.EndPoint;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.ListenerName;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import scala.collection.Seq;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.concurrent.ExecutionException;

/**
 * Prints assignments of topic partition replicas to brokers.
 * <br />
 * The output format is JSON, and can be directly fed into the Kafka partition assigner tool.
 * <br />
 * Usage:
 * <code>
 *     bin/kafka-assignment-generator.sh
 *     --zk_string zkhost:2181
 *     --mode PRINT_REASSIGNMENT
 *     --broker_hosts host1,host2,host3
 *     --broker_hosts_to_remove misbehaving_host1
 * </code>
 */
public class KafkaAssignmentGenerator {
    private static final int KAFKA_FORMAT_VERSION = 1;

    private static final Splitter SPLITTER = Splitter.on(',');

    @Option(name = "--bootstrap_servers",
            usage = "bootstrap servers as comma-separated host:port pairs")
    private String bootstrapServers = null;

    @Option(name = "--mode",
            usage = "the mode to run (PRINT_CURRENT_ASSIGNMENT, PRINT_CURRENT_BROKERS, " +
                    "PRINT_REASSIGNMENT)")
    private Mode mode = null;

    @Option(name = "--integer_broker_ids",
            usage = "comma-separated list of Kafka broker IDs (integers)")
    private String brokerIds = null;

    @Option(name = "--broker_hosts",
            usage = "comma-separated list of broker hostnames (instead of broker IDs)")
    private String brokerHostnames = null;

    @Option(name = "--broker_hosts_to_remove",
            usage = "comma-separated list of broker hostnames to exclude (instead of broker IDs)")
    private String brokerHostnamesToReplace = null;

    @Option(name = "--topics",
            usage = "comma-separated list of topics")
    private String topics = null;

    @Option(name = "--desired_replication_factor",
            usage = "used for changing replication factor for topics, if not present it will use the existing number")
    private int desiredReplicationFactor = -1;

    @Option(name = "--disable_rack_awareness",
            usage = "set to true to ignore rack configurations")
    private boolean disableRackAwareness = false;

    private enum Mode {
        /**
         * Get a snapshot of assigned partitions in Kafka-parseable JSON.
         */
        PRINT_CURRENT_ASSIGNMENT,

        /**
         * Get a list of brokers in the cluster, including their Kafka ID and host:port.
         */
        PRINT_CURRENT_BROKERS,

        /**
         * Get a near-optimal reassignment with minimal partition movement.
         */
        PRINT_REASSIGNMENT
    }

    private static void printCurrentAssignment(AdminClient adminClient, List<String> specifiedTopics) {
        List<String> topics = specifiedTopics != null ? specifiedTopics : getAllTopics(adminClient);
        System.out.println("CURRENT ASSIGNMENT:");
        System.out.println(
                formatAsReassignmentJson(adminClient, topics));
    }

    private static void printCurrentBrokers(AdminClient adminClient) throws JSONException {
        //List<Broker> brokers = JavaConversions.seqAsJavaList(zkUtils.getAllBrokersInCluster());
        List<Broker> brokers = getAllBrokersInCluster(adminClient);
        JSONArray json = new JSONArray();
        for (Broker broker : brokers) {
            BrokerEndPoint endpoint = broker.brokerEndPoint(ListenerName
                    .forSecurityProtocol(SecurityProtocol.PLAINTEXT));

            JSONObject brokerJson = new JSONObject();
            brokerJson.put("id", broker.id());
            brokerJson.put("host", endpoint.host());
            brokerJson.put("port", endpoint.port());
            if (broker.rack().isDefined()) {
                brokerJson.put("rack", broker.rack().get());
            }
            json.put(brokerJson);
        }
        System.out.println("CURRENT BROKERS:");
        System.out.println(json.toString());
    }

    private static void printLeastDisruptiveReassignment(
            AdminClient adminClient, List<String> specifiedTopics, Set<Integer> specifiedBrokers,
            Set<Integer> excludedBrokers, Map<Integer, String> rackAssignment, int desiredReplicationFactor)
            throws JSONException {
        // We need three inputs for rebalacing: the brokers, the topics, and the current assignment
        // of topics to brokers.
        Set<Integer> brokerSet = specifiedBrokers;
        if (brokerSet == null || brokerSet.isEmpty()) {
            brokerSet = Sets.newHashSet(Lists.transform(
                    getAllBrokersInCluster(adminClient), new Function<Broker, Integer>() {
                        public @Nullable Integer apply(@Nullable Broker broker) {
                            return broker.id();
                        }
                    }));
        }

        // Exclude brokers that we want to decommission
        Set<Integer> brokers = Sets.difference(brokerSet, excludedBrokers);
        rackAssignment.keySet().retainAll(brokers);

        // The most common use case is to rebalance all topics, but explicit topic addition is also
        // supported.
        List<String> topics = specifiedTopics != null ?
                specifiedTopics : getAllTopics(adminClient);

        // Print the current assignment in case a rollback is needed
        printCurrentAssignment(adminClient, topics);

//        Map<String, Map<Integer, List<Integer>>> initialAssignments =
//                KafkaTopicAssigner.topicMapToJavaMap(getPartitionAssignmentForTopics(adminClient,
//                        topics));

        Map<String, Map<Integer, List<Integer>>> initialAssignments = getReplicaAssignmentForTopics(adminClient,
                        topics);


        // Assign topics one at a time. This is slightly suboptimal from a packing standpoint, but
        // it's close enough to work in practice. We can also always follow it up with a Kafka
        // leader election rebalance if necessary.
        JSONObject json = new JSONObject();
        json.put("version", KAFKA_FORMAT_VERSION);
        JSONArray partitionsJson = new JSONArray();
        KafkaTopicAssigner assigner = new KafkaTopicAssigner();
        for (String topic : topics) {
            Map<Integer, List<Integer>> partitionAssignment = initialAssignments.get(topic);
            Map<Integer, List<Integer>> finalAssignment = assigner.generateAssignment(
                    topic, partitionAssignment, brokers, rackAssignment, desiredReplicationFactor);
            for (Map.Entry<Integer, List<Integer>> e : finalAssignment.entrySet()) {
                JSONObject partitionJson = new JSONObject();
                partitionJson.put("topic", topic);
                partitionJson.put("partition", e.getKey());
                partitionJson.put("replicas", new JSONArray(e.getValue()));
                partitionsJson.put(partitionJson);
            }
        }
        json.put("partitions", partitionsJson);
        System.out.println("NEW ASSIGNMENT:\n" + json.toString());
    }

    private static Set<Integer> brokerHostnamesToBrokerIds(
            AdminClient adminClient, Set<String> brokerHostnameSet, boolean checkPresence) {
        List<Broker> brokers = getAllBrokersInCluster(adminClient);
        Set<Integer> brokerIdSet = Sets.newHashSet();
        for (Broker broker : brokers) {
            BrokerEndPoint endpoint = broker.brokerEndPoint(ListenerName.
                    forSecurityProtocol(SecurityProtocol.PLAINTEXT));
            if (brokerHostnameSet.contains(endpoint.host())) {
                brokerIdSet.add(broker.id());
            }
        }
        Preconditions.checkArgument(!checkPresence ||
                brokerHostnameSet.size() == brokerIdSet.size(),
                "Some hostnames could not be found! We found: " + brokerIdSet);

        return brokerIdSet;
    }

    private Set<Integer> getBrokerIds(AdminClient adminClient) {
        Set<Integer> brokerIdSet = Collections.emptySet();
        if (StringUtils.isNotEmpty(brokerIds)) {
            brokerIdSet = ImmutableSet.copyOf(Iterables.transform(SPLITTER.split(brokerIds), new Function<String, Integer>() {
                public @Nullable Integer apply(@Nullable String brokerId) {
                    try {
                        return Integer.parseInt(brokerId);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid broker ID: " + brokerId);
                    }
                }
            }));
        } else if (StringUtils.isNotEmpty(brokerHostnames)) {
            Set<String> brokerHostnameSet = ImmutableSet.copyOf(SPLITTER.split(brokerHostnames));
            brokerIdSet = brokerHostnamesToBrokerIds(adminClient, brokerHostnameSet, true);
        }
        return brokerIdSet;
    }

    private Set<Integer> getExcludedBrokerIds(AdminClient adminClient) {
        if (StringUtils.isNotEmpty(brokerHostnamesToReplace)) {
            Set<String> brokerHostnamesToReplaceSet = ImmutableSet.copyOf(
                    SPLITTER.split(brokerHostnamesToReplace));
            return ImmutableSet.copyOf(
                    brokerHostnamesToBrokerIds(adminClient, brokerHostnamesToReplaceSet, false));

        }
        return Collections.emptySet();
    }

    private Map<Integer, String> getRackAssignment(AdminClient adminClient) {
        List<Broker> brokers  = getAllBrokersInCluster(adminClient);
        Map<Integer, String> rackAssignment = Maps.newHashMap();
        if (!disableRackAwareness) {
            for (Broker broker : brokers) {
                scala.Option<String> rack = broker.rack();
                if (rack.isDefined()) {
                    rackAssignment.put(broker.id(), rack.get());
                }
            }
        }
        return rackAssignment;
    }

    private List<String> getTopics() {
        return topics != null ? Lists.newLinkedList(SPLITTER.split(topics)) : null;
    }

    private void runTool(String[] args) throws JSONException {
        // Get and validate all arguments from args4j
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
            Preconditions.checkNotNull(bootstrapServers);
            Preconditions.checkNotNull(mode);
            Preconditions.checkArgument(brokerIds == null || brokerHostnames == null,
                    "--kafka_assigner_integer_broker_ids and " +
                            "--kafka_assigner_broker_hosts cannot be used together!");
        } catch (Exception e) {
            System.err.println("./kafka-assignment-generator.sh [options...] arguments...");
            parser.printUsage(System.err);
            return;
        }
        List<String> topics = getTopics();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AdminClient adminClient = AdminClient.create(props);

        try {
            Set<Integer> brokerIdSet = getBrokerIds(adminClient);
            Set<Integer> excludedBrokerIdSet = getExcludedBrokerIds(adminClient);
            Map<Integer, String> rackAssignment = getRackAssignment(adminClient);
            switch (mode) {
                case PRINT_CURRENT_ASSIGNMENT:
                    printCurrentAssignment(adminClient, topics);
                    break;
                case PRINT_CURRENT_BROKERS:
                    printCurrentBrokers(adminClient);
                    break;
                case PRINT_REASSIGNMENT:
                    printLeastDisruptiveReassignment(adminClient, topics, brokerIdSet,
                            excludedBrokerIdSet, rackAssignment, desiredReplicationFactor);
                    break;
                default:
                    throw new UnsupportedOperationException("Invalid mode: " + mode);
            }
        } finally {
            adminClient.close();
        }
    }

    private static List<Broker> getAllBrokersInCluster( AdminClient adminClient) {
        try {
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            List<Broker> brokersList = new ArrayList<Broker>();
            clusterResult.nodes().get().forEach(node -> {
                EndPoint endPoint = new EndPoint(node.host(), node.port()
                        , ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
                        ,SecurityProtocol.PLAINTEXT);
                Seq<EndPoint> endPointSeq = scala.collection.JavaConverters.asScalaBuffer(Collections.singletonList(endPoint)).toSeq();
                scala.Option<String> rack = scala.Option.apply(node.rack());
                brokersList.add(new Broker(node.id(), endPointSeq, rack));
            });
            return brokersList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    private static List<String> getAllTopics(AdminClient adminClient){
        List<String> topicList = new ArrayList<String>();
        try {
            adminClient.listTopics().names().get().forEach(topic -> {
                topicList.add(topic);
            });

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return topicList;
    }

    private static Map<String, Map<Integer, List<Integer>>> getReplicaAssignmentForTopics(
            AdminClient adminClient, List<String> topics) {
        Map<String, Map<Integer, List<Integer>>> replicaAssignment = Maps.newHashMap();
        try {
            Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(topics).all().get();
            topicDescriptions.forEach((topic, description) -> {
                Map<Integer, List<Integer>> partitionAssignment = Maps.newHashMap();
                description.partitions().forEach(partition -> {
                    List<Integer> replicas = new ArrayList<Integer>();
                    partition.replicas().forEach(replica -> {
                        replicas.add(replica.id());
                    });
                    partitionAssignment.put(partition.partition(), replicas);
                });
                replicaAssignment.put(topic, partitionAssignment);
            });

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return replicaAssignment;
    }

    public static JSONObject formatAsReassignmentJson( AdminClient adminClient, List<String> topics) {
        JSONObject reassignmentJson = new JSONObject();
        Map<String, TopicDescription> topicDescriptions = null;
        try {
            topicDescriptions = adminClient.describeTopics(topics).all().get();

            topicDescriptions.forEach((topicName, description) -> {
                description.partitions().forEach(partitionInfo -> {
                    JSONObject partitionObj = new JSONObject();
                    partitionObj.put("topic", topicName);
                    partitionObj.put("partition", partitionInfo.partition());
                    List<Integer> replicas = new ArrayList<Integer>();
                    for (Node replica : partitionInfo.replicas()) {
                        replicas.add(replica.id());
                    }
                    partitionObj.put("replicas",replicas);

                    reassignmentJson.append("partitions", partitionObj);
                });
            });
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        return reassignmentJson;
    }

    public static void main(String[] args) throws JSONException {
        new KafkaAssignmentGenerator().runTool(args);
    }
}
