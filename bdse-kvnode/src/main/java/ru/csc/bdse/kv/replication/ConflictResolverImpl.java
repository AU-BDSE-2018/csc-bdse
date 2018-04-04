package ru.csc.bdse.kv.replication;

import com.google.protobuf.Timestamp;
import ru.csc.bdse.serialization.Proto;

import java.util.*;
import java.util.stream.Collectors;

public final class ConflictResolverImpl implements ConflictResolver {

    /**
     * {@inheritDoc}
     *
     * the record which is the real value according to the following rules:
     *      1. Compare records' timestamps. If only one record has maximum timestamp, return it.
     *      2. If more than one records have maximum timestamp, take the set of their values
     *      {@link Proto.RecordWithTimestamp#getValue()} and find the most common one. Return
     *      any record with this value.
     *      3. If multiple different values are the most common ones (i.e. the occur the same number of times)
     *      pick the one which was sent by the node with minimum ID.
     */
    @Override
    public Proto.RecordWithTimestamp resolve(Map<Integer, Proto.RecordWithTimestamp> responses) {
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

        final Map<Proto.RecordWithTimestamp, List<Integer>> responseToNodesMap = getResponseToNodesMap(responses);
        final int mostCommonCnt = responseToNodesMap.values()
                .stream()
                .map(List::size)
                .max(Integer::compare)
                .orElseThrow(() -> new RuntimeException("impossible"));

        // leave only most common records
        final Map<Proto.RecordWithTimestamp, List<Integer>> mostCommonRecords = responseToNodesMap.entrySet()
                .stream()
                .filter(entry -> entry.getValue().size() == mostCommonCnt)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (mostCommonRecords.size() == 1) {
            return mostCommonRecords.keySet().iterator().next();
        }

        // rule #3

        final int nodeToUse = responses.entrySet()
                .stream()
                .filter(e -> mostCommonRecords.containsKey(e.getValue()))
                .map(Map.Entry::getKey)
                .sorted()
                .collect(Collectors.toList())
                .iterator().next();
        return responses.get(nodeToUse);
    }

    /**
     * {@inheritDoc}
     *
     * This implementation just joins all sets into one set.
     */
    @Override
    public Set<String> resolveKeys(Map<Integer, Set<String>> responses) {
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

    private Map<Proto.RecordWithTimestamp, List<Integer>> getResponseToNodesMap(Map<Integer, Proto.RecordWithTimestamp> responses) {
        return responses.entrySet()
                .stream()
                .collect(HashMap::new,
                        (map, entry) -> {
                            if (!map.containsKey(entry.getValue())) {
                                List<Integer> nodeId = new ArrayList<>();
                                nodeId.add(entry.getKey());
                                map.put(entry.getValue(), nodeId);
                            } else {
                                map.get(entry.getValue()).add(entry.getKey());
                            }
                        },
                        Map::putAll);
    }

}
