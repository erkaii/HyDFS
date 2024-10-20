package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"FailureDetector/membership"
	"FailureDetector/receiver"
	"FailureDetector/sender"
)

const (
	N             = 10              // Number of machines
	K             = 3               // Number of random machines to ask for a ping
	Timeout       = 2 * time.Second // Timeout for receiving an ACK
	RepingTimeout = 2 * time.Second
)

var suschan = make(chan bool)
var sus_mode = false

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <vm_number>")
	}

	logFile, err := os.OpenFile("machine.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %s", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)

	// Get the second argument which is the VM number
	vmNumber, err := strconv.Atoi(os.Args[1])
	if err != nil || vmNumber < 1 || vmNumber > N {
		log.Fatal("The VM number must be an integer between 1 and 10.")
	}

	// Construct the domain name based on the VM number
	domain := "fa24-cs425-68" + fmt.Sprintf("%02d", vmNumber) + ".cs.illinois.edu"

	ml := membership.NewMembershipList()

	go handleSus()
	go startListenPing(domain, ml)
	go startListenPingRequest(domain, ml)
	go startListenGossiping(domain, ml)
	go startListenCmd(domain, ml)
	go updateFailure(domain, ml)
	go startFailureDetect(ml, domain)

	// Signal handling for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	// User input loop for commands
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		command := strings.TrimSpace(input)

		switch command {
		case "list_mem":
			ml.Display()
		case "list_id":
			member, exists := ml.GetMember(domain)

			if !exists {
				fmt.Println("You haven't join the network :(")
			} else {
				fmt.Println(member.IP + " " + member.Timestamp.String())
			}
		case "join":
			if len(ml.Members) > 0 {
				fmt.Println("Failed to join, you are already in the network!")
				continue
			}
			s := sender.NewSender(sender.IntroducerAddr, sender.GossipPort, domain)
			err := s.Ping(10 * time.Second)
			if err != nil {
				fmt.Println("Failed to join, introducer offline")
			} else {
				err := s.Gossip(time.Now(), domain, "JOIN", domain, 0)
				if err != nil {
					feedback := err.Error()
					if strings.HasPrefix(feedback, "APPROVED") {
						fmt.Println("Joined!")
						var copyMembership string
						fmt.Sscanf(feedback, "APPROVED %s", &copyMembership)
						ml.Parse(copyMembership)
					} else {
						fmt.Println("Failed to join, introducer not in network")
					}
				}
			}
		case "leave":
			if len(ml.Members) == 0 {
				fmt.Println("Failed to leave, you are not in the network!")
				continue
			}
			gMembers := ml.GetRandomMembers(sender.G, []string{domain, domain})
			log.Printf("Gossiping leave of myself with: \n")
			for i, gMember := range gMembers {
				log.Println(i, gMember.IP)
				gSender := sender.NewSender(gMember.IP, sender.GossipPort, domain)
				if err := gSender.Gossip(time.Now(), domain, "FAILED", domain, 0); err != nil {
					log.Printf("Failed to send gossip to %s.\n", gMember.IP)
				}
			}
			ml.Clear()
			log.Println("Left the network with leave command.")
			fmt.Println("You have left the network")
		case "enable_sus":
			for vmNumber = 1; vmNumber < 11; vmNumber++ {
				vmdomain := "fa24-cs425-68" + fmt.Sprintf("%02d", vmNumber) + ".cs.illinois.edu"
				cSender := sender.NewSender(vmdomain, sender.CmdPort, domain)
				if err := cSender.Cmd("CMD ON"); err != nil {
					log.Printf("Failed to send command to %s. With error: %s\n", vmdomain, err.Error())
				}
			}
			fmt.Println("Suspicion mode on")
		case "disable_sus":
			for vmNumber = 1; vmNumber < 11; vmNumber++ {
				vmdomain := "fa24-cs425-68" + fmt.Sprintf("%02d", vmNumber) + ".cs.illinois.edu"
				cSender := sender.NewSender(vmdomain, sender.CmdPort, domain)
				if err := cSender.Cmd("CMD OFF"); err != nil {
					log.Printf("Failed to send command to %s. With error: %s\n", vmdomain, err.Error())
				}
			}
			fmt.Println("Suspicion mode off")
		case "status_sus":
			fmt.Println(sus_mode)
		case "quit":
			log.Println("Left the network with quit command.")
			fmt.Println("Shutting down.")
			return
		case "rate_0":
			for vmNumber = 1; vmNumber < 11; vmNumber++ {
				vmdomain := "fa24-cs425-68" + fmt.Sprintf("%02d", vmNumber) + ".cs.illinois.edu"
				cSender := sender.NewSender(vmdomain, sender.CmdPort, domain)
				if err := cSender.Cmd("CMD 0.0"); err != nil {
					log.Printf("Failed to send command to %s. With error: %s\n", vmdomain, err.Error())
				}
			}
		case "rate_1":
			for vmNumber = 1; vmNumber < 11; vmNumber++ {
				vmdomain := "fa24-cs425-68" + fmt.Sprintf("%02d", vmNumber) + ".cs.illinois.edu"
				cSender := sender.NewSender(vmdomain, sender.CmdPort, domain)
				if err := cSender.Cmd("CMD 0.01"); err != nil {
					log.Printf("Failed to send command to %s. With error: %s\n", vmdomain, err.Error())
				}
			}
		case "rate_5":
			for vmNumber = 1; vmNumber < 11; vmNumber++ {
				vmdomain := "fa24-cs425-68" + fmt.Sprintf("%02d", vmNumber) + ".cs.illinois.edu"
				cSender := sender.NewSender(vmdomain, sender.CmdPort, domain)
				if err := cSender.Cmd("CMD 0.05"); err != nil {
					log.Printf("Failed to send command to %s. With error: %s\n", vmdomain, err.Error())
				}
			}
		case "rate_10":
			for vmNumber = 1; vmNumber < 11; vmNumber++ {
				vmdomain := "fa24-cs425-68" + fmt.Sprintf("%02d", vmNumber) + ".cs.illinois.edu"
				cSender := sender.NewSender(vmdomain, sender.CmdPort, domain)
				if err := cSender.Cmd("CMD 0.1"); err != nil {
					log.Printf("Failed to send command to %s. With error: %s\n", vmdomain, err.Error())
				}
			}
		case "rate_15":
			for vmNumber = 1; vmNumber < 11; vmNumber++ {
				vmdomain := "fa24-cs425-68" + fmt.Sprintf("%02d", vmNumber) + ".cs.illinois.edu"
				cSender := sender.NewSender(vmdomain, sender.CmdPort, domain)
				if err := cSender.Cmd("CMD 0.15"); err != nil {
					log.Printf("Failed to send command to %s. With error: %s\n", vmdomain, err.Error())
				}
			}
		case "rate_20":
			for vmNumber = 1; vmNumber < 11; vmNumber++ {
				vmdomain := "fa24-cs425-68" + fmt.Sprintf("%02d", vmNumber) + ".cs.illinois.edu"
				cSender := sender.NewSender(vmdomain, sender.CmdPort, domain)
				if err := cSender.Cmd("CMD 0.2"); err != nil {
					log.Printf("Failed to send command to %s. With error: %s\n", vmdomain, err.Error())
				}
			}
		default:
			fmt.Println("Unknown command.")
		}

		select {
		case <-c:
			fmt.Println("\nReceived interrupt signal, shutting down.")
			return
		default:
			// Continue to next iteration for user input
		}
	}
}

func startListenPing(myDomain string, ml *membership.MembershipList) {
	r := receiver.NewReceiver(myDomain, sender.PingPort)
	go r.Listen(ml, suschan)
}

func startListenPingRequest(myDomain string, ml *membership.MembershipList) {
	r := receiver.NewReceiver(myDomain, sender.RepingPort)
	go r.Listen(ml, suschan)
}

func startListenGossiping(myDomain string, ml *membership.MembershipList) {
	r := receiver.NewReceiver(myDomain, sender.GossipPort)
	go r.Listen(ml, suschan)
}

func startListenCmd(myDomain string, ml *membership.MembershipList) {
	r := receiver.NewReceiver(myDomain, sender.CmdPort)
	go r.Listen(ml, suschan)
}

func handleSus() {
	for {
		select {
		case suspend := <-suschan:
			sus_mode = suspend
		}
	}
}

func updateFailure(myDomain string, ml *membership.MembershipList) {
	for {
		// Update all the suspected nodes to failed if timeout
		currentTime := time.Now()
		for _, checkmember := range ml.Members {
			if checkmember.State == membership.Suspected && currentTime.Sub(checkmember.Timestamp) >= sender.Sus_timeout {
				log.Printf("Failure confirmed for %s due to timeout.\n", checkmember.IP)
				ml.UpdateMember(checkmember.IP, membership.Failed, time.Now(), ml.GetIncNumber(checkmember.IP))

				gMembers := ml.GetRandomMembers(sender.G, []string{myDomain, checkmember.IP})
				log.Printf("Gossiping failure confirmation of %s with: \n", checkmember.IP)
				for i, gMember := range gMembers {
					log.Println(i, gMember.IP)
					gSender := sender.NewSender(gMember.IP, sender.GossipPort, myDomain)
					if err := gSender.Gossip(time.Now(), checkmember.IP, "FAILED", myDomain, ml.GetIncNumber(checkmember.IP)); err != nil {
						log.Printf("Failed to send gossip to %s.\n", gMember.IP)
					}
				}
			}
		}
	}
}

func startFailureDetect(ml *membership.MembershipList, myDomain string) {
	for {
		if len(ml.Members) == 0 { // Haven't join the network yet
			time.Sleep(sender.FD_period)
			continue
		}

		// Randomly select a member to ping
		member := ml.RandomMember(myDomain)
		if member == nil {
			log.Println("No members available to ping.")
			time.Sleep(sender.FD_period)
			continue
		}

		log.Printf("Pinging %s...\n", member.IP)

		// Create a sender for the selected member
		s := sender.NewSender(member.IP, sender.PingPort, myDomain)
		err := s.Ping(Timeout)
		if err != nil {
			log.Printf("Ping to %s failed: %s\n", member.IP, err)
			kMembers := ml.GetRandomMembers(K, []string{myDomain, member.IP})
			ackReceived := false

			log.Println("Checking live status of " + member.IP + " with")

			for i, kMember := range kMembers {
				log.Println(i, kMember.IP)
				kSender := sender.NewSender(kMember.IP, sender.RepingPort, myDomain)
				if err := kSender.Reping(RepingTimeout, member.IP); err == nil {
					ackReceived = true
					break
				}
			}

			if !ackReceived {
				// log.Printf("Marking %s as failed.\n", member.IP)
				updatedState := membership.Failed
				gossipCmd := "FAILED"
				if sus_mode {
					if member.State == membership.Suspected {
						continue
					}
					updatedState = membership.Suspected
					gossipCmd = "SUSPECTED"
				}

				ml.UpdateMember(member.IP, updatedState, time.Now(), ml.GetIncNumber(member.IP))

				gMembers := ml.GetRandomMembers(sender.G, []string{myDomain, member.IP})
				log.Printf("Gossiping failure/suspect of %s with: \n", member.IP)
				log.Printf("Failure detection/suspicion of %s at %s\n", member.IP, time.Now())
				for i, gMember := range gMembers {
					log.Println(i, gMember.IP)
					gSender := sender.NewSender(gMember.IP, sender.GossipPort, myDomain)
					if err := gSender.Gossip(time.Now(), member.IP, gossipCmd, myDomain, ml.GetIncNumber(member.IP)); err != nil {
						log.Printf("Failed to send gossip to %s.\n", gMember.IP)
					}
				}
			}
		}

		time.Sleep(sender.FD_period) // Wait before the next ping
	}
}
