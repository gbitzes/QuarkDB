# HMAC signing challenges

HMAC signing challenges are a better alternative to [AUTH](auth.md) for a client to prove
knowledge of the password, without having to actually supply the password over the wire.

Due to its complexity, this method of authentication is only viable through scripts
and code, not interactively through the CLI.

Client and server agree together on a string-to-sign, and the client proves knowledge
of the password by supplying the correct HMAC EVP sha256 signature of the generated
string.

The procedure has been implemented as a QClient handshake, use `HmacAuthHandshake`
for convenience.

## Example

The client asks the server to generate a signing challenge, providing 64 random
bytes:

```
localhost:7777> HMAC-AUTH-GENERATE-CHALLENGE <... 64 random bytes ...>
"<... long string to sign...>"
```

The client then signs the server-provided string using HMAC EVP sha256 and
the password it knows about:

```
localhost:7777> HMAC-AUTH-VALIDATE-CHALLENGE <... hmac signature ...>
OK
```

The server validates the signature, and if the signatures match, lets the
client through.
