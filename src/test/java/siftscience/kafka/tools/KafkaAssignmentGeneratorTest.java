package siftscience.kafka.tools;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class KafkaAssignmentGeneratorTest {

    @org.junit.Test
    public void testGenerateAssignment() throws Exception {


        JSONObject reassignmentJson = new JSONObject();
        JSONObject partitionObj = new JSONObject();
        partitionObj.put("topic", "eee");
        partitionObj.put("partition", 1);
        List<Integer> replicas = new ArrayList<Integer>();
        for (int y : Collections.singletonList(1)) {
            replicas.add(y);
        }
        partitionObj.put("replicas",replicas);
        reassignmentJson.append("partitions", partitionObj);


        System.out.println(reassignmentJson);
        assertNotNull(reassignmentJson);
    }


}