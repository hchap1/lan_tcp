# lan_tcp
A simple crate for building silly little networking challenges.
Establishes a server on the LAN through which packets are exchanged with all connected clients.

Broadcasts a UDP message alerting any similar servers on the LAN.
If a server receives the UDP broadcast it will respond to the client directly with its address information, with which the client forms a TCP connection.
If the client does not receive a response from the server within a period of time, it starts its own server and begins listening for UDP broadcasts.

Once a network is established, the goal is to provide each client with the ability to message one, multiple or all other clients by sending a single packet to the server. The server itself exposes an interface causing it to act like a client also - that is, messages can be addressed to or sent from the server.
The difference between client and server is entirely obfuscated within a "Node" architecture.

Connections will be terminated by Server/Client(s) when an incorrect 'identifier' str is exchanged to provide some layer of resistance to random things connecting.

Long term goals:
- Collision resistance (IE multiple applications employing this crate on the same port).
- Thorough testing and optimisation for throughput / large number of clients
- Realtime restructuring when server exits via election of a new server
