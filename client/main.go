package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Message struct { From, Text, Kind string; Timestamp time.Time }
type JoinArgs struct { Username, ClientID string }
type JoinReply struct { OK bool; Error string }
type PollArgs struct { Username, ClientID string }
type PollReply struct { Messages []Message; Error string }
type SendArgs struct { Username, ClientID, Text string }
type SendReply struct { OK bool; Error string }
type LeaveArgs struct { Username, ClientID string }
type LeaveReply struct { OK bool; Error string }
type ListArgs struct{}
type ListReply struct { Users []string }
type input struct { line string; ok bool }

func call(client *rpc.Client, method string, args, reply any) error { return client.Call("Chat."+method, args, reply) }
func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "chat server address"); flag.Parse()
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("Username: "); if !scanner.Scan() { return }; username := strings.TrimSpace(scanner.Text())
	if username == "" { log.Fatal("username cannot be empty") }
	client, err := rpc.Dial("tcp", *addr); if err != nil { log.Fatalf("connect to %s: %v", *addr, err) }; defer client.Close()
	var idBytes [16]byte; if _, err := rand.Read(idBytes[:]); err != nil { log.Fatal(err) }; clientID := hex.EncodeToString(idBytes[:])
	var joined JoinReply; if err := call(client, "Join", JoinArgs{username, clientID}, &joined); err != nil { log.Fatalf("join: %v", err) }; if !joined.OK { log.Fatal(joined.Error) }
	fmt.Printf("Welcome, %s! Type a message and press Enter. Commands: /users, /quit\n", username)
	lines := make(chan input); go func() { for scanner.Scan() { lines <- input{line:scanner.Text(), ok:true} }; lines <- input{ok:false} }()
	stop := make(chan os.Signal, 1); signal.Notify(stop, os.Interrupt, syscall.SIGTERM); defer signal.Stop(stop)
	pollErr := make(chan error, 1); go func() {
		ticker := time.NewTicker(250*time.Millisecond); defer ticker.Stop()
		for range ticker.C { var reply PollReply; if err := call(client, "Poll", PollArgs{username, clientID}, &reply); err != nil { pollErr <- err; return }; if reply.Error != "" { pollErr <- fmt.Errorf("%s", reply.Error); return }; for _, m := range reply.Messages { stamp := m.Timestamp.Local().Format("15:04:05"); if m.Kind == "system" { fmt.Printf("\n[%s] * %s\n", stamp, m.Text) } else { fmt.Printf("\n[%s] %s: %s\n", stamp, m.From, m.Text) }; fmt.Print("> ") } }
	}()
	leave := func() { var reply LeaveReply; _ = call(client, "Leave", LeaveArgs{username, clientID}, &reply) }
	for { fmt.Print("> "); select {
	case <-stop: fmt.Println("\nDisconnecting..."); leave(); return
	case err := <-pollErr: fmt.Printf("\nDisconnected from server: %v\n", err); return
	case in := <-lines:
		if !in.ok { leave(); return }; text := strings.TrimSpace(in.line)
		switch text {
		case "": continue
		case "/quit": leave(); fmt.Println("Goodbye."); return
		case "/users": var reply ListReply; if err := call(client, "List", ListArgs{}, &reply); err != nil { fmt.Printf("Unable to list users: %v\n", err) } else { fmt.Printf("Online (%d): %s\n", len(reply.Users), strings.Join(reply.Users, ", ")) }
		default: var reply SendReply; if err := call(client, "Send", SendArgs{username, clientID, text}, &reply); err != nil { fmt.Printf("Unable to send message: %v\n", err) } else if !reply.OK { fmt.Println(reply.Error) }
		}
	} }
}

