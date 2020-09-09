# AUTH password

Authenticates the given client connection: The client proves knowledge of the
password by supplying it directly to the server.

```
127.0.0.1:4445> get key
(error) NOAUTH Authentication required.
127.0.0.1:4445> auth wrong_password
(error) ERR invalid password
127.0.0.1:4445> auth correct_password
OK
127.0.0.1:4445> get key
"contents"
```

The password should be supplied each time a reconnection occurs. In QClient,
reconnections from transient network failures are handled transparently -- the password
should be supplied as part of the handshake, not through a regular command.