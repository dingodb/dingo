# Network protocols

## Data messages and control messages

There are two types of messages transferred among the cluster nodes: data messages and control messages. Data messages
contains encoded tuples delivered by the "send operators", including "FIN". Control messages are used to coordinate the
job running. The following is the control message types currently implemented

1. Task delivery message: tasks are serialized and delivered from the coordinator to executors.
2. Receiving ready message: the "receive operators" send messages to the corresponding "send operators" to notify that
   they are properly started and ready to receive data messages.

The interaction between two tasks which exchanging data with each other is illustrated in the following figure.

```{mermaid}
sequenceDiagram
    participant R as Task with receive opertaor
    participant S as Task with send operator
    R ->> S: RCV_READY
    loop Data transfer
        S ->> R: Send tuples
    end
    S ->> R: Send FIN
```

## Serialize and deserialize

The tuples are encoded in Avro format into data messages.

The tasks are encoded in JSON format.
