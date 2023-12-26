package io.dingodb.exec.utils;


import io.dingodb.exec.transaction.util.TransactionDiGraph;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TxnDiGraphTest {

    @Test
    public void detect() {

        class Testcase {
            final String[] edges;
            final Integer[] pointsOnCycle;

            public Testcase(String[] edges, Integer[] pointsOnCycle) {
                this.edges = edges;
                this.pointsOnCycle = pointsOnCycle;
            }
        }

        Testcase[] testcases = new Testcase[] {
            // one cycle.
            new Testcase(new String[] {
                "1->2",
                "2->3",
                "3->4",
                "4->5",
                "5->6",
                "6->1",
            }, new Integer[] {
                1, 2, 3, 4, 5, 6,
            }),
            // one cycle with branch.
            new Testcase(new String[] {
                "1->2",
                "2->3",
                "3->4",
                "4->5",
                "5->6",
                "6->7",
                "7->8",
                "6->1",
            }, new Integer[] {1, 2, 3, 4, 5, 6}),
            new Testcase(new String[] {
                "1->2",
                "2->3",
                "3->1",
                "3->4",
                "4->5",
                "5->3",
                "6->7",
                "8->9",
            }, new Integer[] {1, 2, 3}),
            new Testcase(new String[] {
                "1->2",
                "3->4",
                "4->3",
            }, new Integer[] {3, 4}),
            new Testcase(new String[] {
                "1->2",
                "2->3",
                "3->4",
            }, new Integer[] {}),
        };

        for (Testcase testcase : testcases) {
            TransactionDiGraph<Integer> graph = new TransactionDiGraph<>();
            for (String edgeDesc : testcase.edges) {
                String[] parts = edgeDesc.split("->");
                Integer from = Integer.valueOf(parts[0]);
                Integer to = Integer.valueOf(parts[1]);
                graph.addDiEdge(from, to);
            }
            Optional<ArrayList<Integer>> result = graph.detect();
            if (testcase.pointsOnCycle.length == 0) {
                assertFalse(result.isPresent());
            } else {
                assertTrue(result.isPresent());
                assertEquals(Arrays.asList(testcase.pointsOnCycle), result.get());
            }
        }
    }
}
