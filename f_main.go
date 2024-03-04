package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
)

// Command line defaults
const (
	DefaultHTTPAddr = "127.0.0.1:11000"
	DefaultRaftAddr = "127.0.0.1:12000"
)

var httpAddr string
var raftAddr string
var joinAddr string
var nodeID string

func main() {
	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
	flag.StringVar(&nodeID, "id", "", "Node ID. If not set, same as Raft bind address")
	flag.Parse()

	// Ensure Raft storage exists.
	raftDir := flag.Arg(0)
	if raftDir == "" {
		log.Fatalln("No Raft storage directory specified")
	}
	if err := os.MkdirAll(raftDir, 0700); err != nil {
		log.Fatalf("failed to create path for Raft storage: %s", err.Error())
	}

	s := NewRaftKVStore()
	s.RaftBind = raftAddr
	s.RaftDir = raftDir
	if err := s.Open(joinAddr == "", nodeID); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	h := NewHttp(httpAddr, s)
	if err := h.Start(); err != nil {
		log.Fatalf("failed to start HTTP service: %s", err.Error())
	}

	if joinAddr != "" {
		if err := join(joinAddr, raftAddr, nodeID); err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		}
	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
}

func join(joinAddr, raftAddr, nodeID string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr, "id": nodeID})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
