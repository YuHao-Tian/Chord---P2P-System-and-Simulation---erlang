# Chord---P2P-System-and-Simulation---erlang
## Team members
Xin Li, Yuhao Tian
## What is working?
1. We have implemented the chord protocol project using Erlang which have been talked about during class.  
2. We are able to do the scalable key location lookup through populating finger table.
3. Through a sequence of hops, each node can send requests to the proper node in the network.
4. There were numerous collisions when creating distinct node ids as a result of the hashing operation as specified in the criteria followed by m bit reductions. To circumvent this difficulty, the project adopts the random number technique.  

## What is the largest network you managed to deal with?
The largest network we have dealt with is 1000 nodes and every node cotains 100 messages. It took an average of [3,5] hops for a message to arrive at its destination. The result was 3.97 hops when the value was averaged over 10 runs. This means that the chord protocol requires an average of 3.97 hops for each node to look for any content.
