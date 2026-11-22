package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const sessionTimeout = 30 * time.Second

type Message struct {
	From      string    `json:"from"`
	Text      string    `json:"text"`
	Kind      string    `json:"kind"` // chat or system
	Timestamp time.Time `json:"timestamp"`
}

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

type user struct { clientID string; inbox []Message; lastSeen time.Time }
type ChatService struct { mu sync.Mutex; users map[string]*user }

func NewChatService() *ChatService { return &ChatService{users: make(map[string]*user)} }
func validUsername(name string) bool { n := strings.TrimSpace(name); return len(n) >= 1 && len(n) <= 24 && !strings.ContainsAny(n, "\r\n\t") }
func (s *ChatService) enqueueLocked(name string, msg Message) { if u := s.users[name]; u != nil { u.inbox = append(u.inbox, msg) } }
func systemMessage(text string) Message { return Message{From:"SYSTEM", Text:text, Kind:"system", Timestamp:time.Now()} }

func (s *ChatService) Join(a JoinArgs, r *JoinReply) error {
	name := strings.TrimSpace(a.Username)
	if !validUsername(name) || a.ClientID == "" { r.Error = "username must be 1-24 characters and client ID is required"; return nil }
	s.mu.Lock(); defer s.mu.Unlock()
	if _, exists := s.users[name]; exists { r.Error = "username is already in use"; return nil }
	for other := range s.users { s.enqueueLocked(other, systemMessage(name+" joined the chat")) }
	s.users[name] = &user{clientID:a.ClientID, lastSeen:time.Now()}; r.OK = true
	log.Printf("%s joined (%d users)", name, len(s.users)); return nil
}
func (s *ChatService) Poll(a PollArgs, r *PollReply) error {
	s.mu.Lock(); defer s.mu.Unlock()
	u := s.users[a.Username]; if u == nil || u.clientID != a.ClientID { r.Error = "session is no longer active"; return nil }
	u.lastSeen = time.Now(); r.Messages = append([]Message(nil), u.inbox...); u.inbox = nil; return nil
}
func (s *ChatService) Send(a SendArgs, r *SendReply) error {
	text := strings.TrimSpace(a.Text); if text == "" { r.Error = "message cannot be empty"; return nil }; if len(text) > 2000 { r.Error = "message is too long (maximum 2000 characters)"; return nil }
	s.mu.Lock(); defer s.mu.Unlock()
	u := s.users[a.Username]; if u == nil || u.clientID != a.ClientID { r.Error = "session is no longer active"; return nil }
	u.lastSeen = time.Now(); msg := Message{From:a.Username, Text:text, Kind:"chat", Timestamp:time.Now()}
	for name := range s.users { if name != a.Username { s.enqueueLocked(name, msg) } }; r.OK = true; return nil
}
func (s *ChatService) removeLocked(name, clientID string) bool {
	u := s.users[name]; if u == nil || (clientID != "" && u.clientID != clientID) { return false }
	delete(s.users, name); for other := range s.users { s.enqueueLocked(other, systemMessage(name+" left the chat")) }
	log.Printf("%s left (%d users)", name, len(s.users)); return true
}
func (s *ChatService) Leave(a LeaveArgs, r *LeaveReply) error { s.mu.Lock(); defer s.mu.Unlock(); r.OK = s.removeLocked(a.Username, a.ClientID); return nil }
func (s *ChatService) List(_ ListArgs, r *ListReply) error {
	s.mu.Lock(); defer s.mu.Unlock(); for name := range s.users { r.Users = append(r.Users, name) }
	// Keep output stable for terminals and automated checks.
	for i := 0; i < len(r.Users); i++ { for j := i+1; j < len(r.Users); j++ { if r.Users[j] < r.Users[i] { r.Users[i], r.Users[j] = r.Users[j], r.Users[i] } } }; return nil
}
func (s *ChatService) reapInactive() {
	t := time.NewTicker(5*time.Second); defer t.Stop()
	for range t.C { s.mu.Lock(); now := time.Now(); for name, u := range s.users { if now.Sub(u.lastSeen) > sessionTimeout { s.removeLocked(name, u.clientID) } }; s.mu.Unlock() }
}
func main() {
	addr := flag.String("addr", ":9000", "TCP address to listen on"); flag.Parse()
	listener, err := net.Listen("tcp", *addr); if err != nil { log.Fatalf("listen: %v", err) }
	svc := NewChatService(); server := rpc.NewServer(); if err := server.RegisterName("Chat", svc); err != nil { log.Fatal(err) }
	go svc.reapInactive(); go func(){ for { conn, err := listener.Accept(); if err != nil { return }; go server.ServeConn(conn) } }()
	log.Printf("chat server listening on %s", *addr)
	sig := make(chan os.Signal, 1); signal.Notify(sig, os.Interrupt, syscall.SIGTERM); <-sig
	log.Print("shutting down server..."); _ = listener.Close()
	svc.mu.Lock(); clear(svc.users); svc.mu.Unlock()
}

