package main

import (
	"HyDFS/failuredetector"
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {

	// Signal handling for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	// Goroutine to handle shutdown signal
	go func() {
		<-c
		fmt.Println("\nReceived interrupt signal, shutting down.")
		os.Exit(0)
	}()

	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <vm_number>")
	}

	logFile, err := os.OpenFile("../machine.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %s", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)

	fmt.Println(HashKey("xyz"))

	ml := failuredetector.NewMembershipList()
	go failuredetector.Failuredetect(ml)

	// User input loop for commands
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		command := strings.TrimSpace(input)

		words := strings.Fields(command)

		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "create":
			if len(words) != 3 {
				fmt.Println("Usage: create localfilename HyDFSfilename")
				continue
			}
		case "get":
			if len(words) != 3 {
				fmt.Println("Usage: get HyDFSfilename localfilename")
				continue
			}

		case "append":
			if len(words) != 3 {
				fmt.Println("Usage: append localfilename HyDFSfilename")
				continue
			}
		case "ls":
			if len(words) != 2 {
				fmt.Println("Usage: ls HyDFSfilename")
				continue
			}
		case "merge":
			if len(words) != 2 {
				fmt.Println("Usage: merge HyDFSfilename")
				continue
			}
		case "store":
			if len(words) != 1 {
				fmt.Println("Usage: store")
				continue
			}
		case "getfromreplica":
			if len(words) != 4 {
				fmt.Println("Usage: getfromreplica VMaddress HyDFSfilename localfilename")
				continue
			}
		case "list_mem_ids":
			if len(words) != 1 {
				fmt.Println("Usage: list_mem_ids")
				continue
			}

		case "list_mem":
			ml.Display()

		default:
			fmt.Println("Unknown command.")
		}
	}
}
