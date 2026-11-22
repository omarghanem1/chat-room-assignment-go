# Concurrent Chat Room (Go RPC)

A terminal chat application with a central server and separate client processes. It uses Go's `net/rpc` over TCP and the polling communication approach.

## Requirements

- Go 1.22 or newer
- Two or more terminal windows

## Run

From the repository root, start the server:

```sh
go run ./server
```

In each other terminal, start a client (repeat for multiple users):

```sh
go run ./client
```

The server listens on `:9000` by default; clients connect to `127.0.0.1:9000`. To choose another address:

```sh
go run ./server -addr :9001
go run ./client -addr 127.0.0.1:9001
```

Type a message and press Enter to broadcast it to everyone else. Use `/users` to list connected users and `/quit` or Ctrl+C to disconnect. The server prints join, leave, and shutdown events. Clients poll every 250 ms for messages and notifications.

## Features

- Unique usernames; duplicate names are rejected.
- Broadcast chat messages to all clients except the sender.
- Join and leave notifications for other clients.
- Concurrent RPC handling, with shared state guarded by a mutex.
- Client sessions expire after 30 seconds without polling, so a crashed client is eventually removed and a leave notification is sent.
- Ctrl+C and `/quit` perform a clean client leave; Ctrl+C stops the server.
- User lists are sorted for consistent display.

## RPC methods

`Chat.Join`, `Chat.Poll`, `Chat.Send`, `Chat.List`, and `Chat.Leave` are registered with `net/rpc`. Each session has a random client ID so an old or duplicate session cannot act as another user's connection.

## Screen recording suggestion

Record the server terminal and at least two client terminals. Show both users joining, run `/users`, exchange messages in both directions, then use `/quit` in one client and show the leave notification in the other.

