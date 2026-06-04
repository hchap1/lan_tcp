# lan_tcp
A simple crate for building silly little networking challenges.
Establishes a server on the LAN through which packets are exchanged with all connected clients.

Broadcasts a UDP message alerting any similar servers on the LAN.
If a server receives the UDP broadcast it will respond to the client directly with its address information, with which the client forms a TCP connection.
If the client does not receive a response from the server within a period of time, it starts its own server and begins listening for UDP broadcasts.

The behaviour of the server is to relay each packet to every client, including itself.

Current behaviour (goal) upon server termination is for every client to terminate.
Connections will be terminated by Server/Client(s) when an incorrect 'identifier' str is exchanged to provide some layer of resistance to random things connecting.

Also, there is no intent to provide collision resistance - that is, two applications sharing the same UDP port will simply fail (unless they have the same identifier).
