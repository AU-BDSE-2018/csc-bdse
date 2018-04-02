package ru.csc.bdse.kv.replication;

import com.google.protobuf.Timestamp;
import ru.csc.bdse.serialization.Proto;

import java.util.*;
import java.util.stream.Collectors;

public final class ConflictResolverImpl implements ConflictResolver {

    @Override
    public Proto.RecordWithTimestamp resolve(Map<String, Proto.RecordWithTimestamp> responses) {
        if (responses.size() == 0) {
            throw new RuntimeException("Need at least one response to resolve conflicts");
        }

        // rule #1

        final Timestamp maxTimestamp = getMaxTimestamp(responses.values());

        // leave only responses with the most up-to-date timestamp
        responses = responses.entrySet()
                .stream()
                .filter(e -> e.getValue().getTimestamp().equals(maxTimestamp))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (responses.size() == 1) {
            return responses.entrySet().iterator().next().getValue();
        }

        // rule #2

        final Map<Proto.RecordWithTimestamp, List<String>> responseToNodesMap = getResponseToNodesMap(responses);
        final int mostCommonCnt = responseToNodesMap.values()
                .stream()
                .map(List::size)
                .max(Integer::compare)
                .orElseThrow(() -> new RuntimeException("impossible"));

        // leave only most common records
        final Map<Proto.RecordWithTimestamp, List<String>> mostCommonRecords = responseToNodesMap.entrySet()
                .stream()
                .filter(entry -> entry.getValue().size() == mostCommonCnt)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (mostCommonRecords.size() == 1) {
            return mostCommonRecords.keySet().iterator().next();
        }

        // rule #3

        final String nodeToUse = responses.entrySet()
                .stream()
                .filter(e -> mostCommonRecords.containsKey(e.getValue()))
                .map(Map.Entry::getKey)
                .sorted()
                .collect(Collectors.toList())
                .iterator().next();
        return responses.get(nodeToUse);
    }

    @Override
    public Set<String> resolveKeys(Map<String, Set<String>> responses) {
        return responses.values()
                .stream()
                .collect(
                        HashSet::new,
                        HashSet::addAll,
                        HashSet::addAll);
    }

    private Timestamp getMaxTimestamp(Collection<Proto.RecordWithTimestamp> responses) {
        final Optional<Timestamp> maxTimestamp = responses
                .stream()
                .map(Proto.RecordWithTimestamp::getTimestamp)
                .max(this::compareTimestamps);
        return maxTimestamp.orElseThrow(() -> new IllegalArgumentException("responses is empty"));
    }

    private int compareTimestamps(Timestamp t1, Timestamp t2) {
        final Long seconds1 = t1.getSeconds();
        final Long seconds2 = t2.getSeconds();
        final Integer nanos1 = t1.getNanos();
        final Integer nanos2 = t2.getNanos();

        if (!seconds1.equals(seconds2)) {
            return seconds1.compareTo(seconds2);
        } else {
            return nanos1.compareTo(nanos2);
        }
    }

    private Map<Proto.RecordWithTimestamp, List<String>> getResponseToNodesMap(Map<String, Proto.RecordWithTimestamp> responses) {
        return responses.entrySet()
                .stream()
                .collect(HashMap::new,
                        (map, entry) -> {
                            if (!map.containsKey(entry.getValue())) {
                                List<String> nodeName = new ArrayList<>();
                                nodeName.add(entry.getKey());
                                map.put(entry.getValue(), nodeName);
                            } else {
                                map.get(entry.getValue()).add(entry.getKey());
                            }
                        },
                        Map::putAll);
    }

}
