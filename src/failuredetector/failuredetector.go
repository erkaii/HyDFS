package failuredetector

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

var (
	N              int
	K              int
	Timeout        time.Duration
	RepingTimeout  time.Duration
	PingPort       string
	RepingPort     string
	GossipPort     string
	CmdPort        string
	G              int
	GossipDuration time.Duration
	IntroducerAddr string
	FD_period      time.Duration
)

func loadConfig(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open config file: %s", err)
	}
	defer file.Close()

	config := make(map[string]interface{})
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		log.Fatalf("Failed to parse config file: %s", err)
	}

	N = config["N"].(int)
	K = config["FD_K"].(int)
	Timeout, _ = time.ParseDuration(config["FD_ping_timeout"].(string))
	RepingTimeout, _ = time.ParseDuration(config["FD_reping_timeout"].(string))
	PingPort = config["FD_ping_port"].(string)
	RepingPort = config["FD_reping_port"].(string)
	GossipPort = config["FD_gossip_port"].(string)
	CmdPort = config["FD_cmd_port"].(string)
	G = config["FD_G"].(int)
	GossipDuration, _ = time.ParseDuration(config["FD_gossip_duration"].(string))
	IntroducerAddr = config["FD_introducer_addr"].(string)
	FD_period, _ = time.ParseDuration(config["FD_fd_period"].(string))
}

func Failuredetect(ml *MembershipList, vmNumber int) {
	loadConfig("../config.yaml")
	// Construct the domain name based on the VM number
	domain := "fa24-cs425-68" + fmt.Sprintf("%02d", vmNumber) + ".cs.illinois.edu"

	// Failure detection go routains
	go startListenPing(domain, ml)
	go startListenPingRequest(domain, ml)
	go startListenGossiping(domain, ml)
	go startListenCmd(domain, ml)
	go startFailureDetect(ml, domain)

	// Wait 0.5s before introducer requests to join itself
	time.Sleep(500 * time.Millisecond)

	// Sent join request automatically
	joinFD(ml, domain)

	for {
		time.Sleep(1 * time.Second)
	}
}

func startListenPing(myDomain string, ml *MembershipList) {
	r := NewReceiver(myDomain, PingPort)
	go r.Listen(ml)
}

func startListenPingRequest(myDomain string, ml *MembershipList) {
	r := NewReceiver(myDomain, RepingPort)
	go r.Listen(ml)
}

func startListenGossiping(myDomain string, ml *MembershipList) {
	r := NewReceiver(myDomain, GossipPort)
	go r.Listen(ml)
}

func startListenCmd(myDomain string, ml *MembershipList) {
	r := NewReceiver(myDomain, CmdPort)
	go r.Listen(ml)
}

func startFailureDetect(ml *MembershipList, myDomain string) {
	for {
		if len(ml.Members) == 0 { // Haven't join the network yet
			time.Sleep(FD_period)
			continue
		}

		// Randomly select a member to ping
		member := ml.RandomMember(myDomain)
		if member == nil {
			log.Println("No members available to ping.")
			time.Sleep(FD_period)
			continue
		}

		log.Printf("Pinging %s...\n", member.IP)

		// Create a sender for the selected member
		s := NewSender(member.IP, PingPort, myDomain)
		err := s.Ping(Timeout)
		if err != nil {
			log.Printf("Ping to %s failed: %s\n", member.IP, err)
			kMembers := ml.GetRandomMembers(K, []string{myDomain, member.IP})
			ackReceived := false

			log.Println("Checking live status of " + member.IP + " with")

			for i, kMember := range kMembers {
				log.Println(i, kMember.IP)
				kSender := NewSender(kMember.IP, RepingPort, myDomain)
				if err := kSender.Reping(RepingTimeout, member.IP); err == nil {
					ackReceived = true
					break
				}
			}

			if !ackReceived {
				// log.Printf("Marking %s as failed.\n", member.IP)
				updatedState := Failed
				gossipCmd := "FAILED"

				ml.UpdateMember(member.IP, updatedState, time.Now(), ml.GetIncNumber(member.IP))

				gMembers := ml.GetRandomMembers(G, []string{myDomain, member.IP})
				log.Printf("Gossiping failure/suspect of %s with: \n", member.IP)
				log.Printf("Failure detection/suspicion of %s at %s\n", member.IP, time.Now())
				for i, gMember := range gMembers {
					log.Println(i, gMember.IP)
					gSender := NewSender(gMember.IP, GossipPort, myDomain)
					if err := gSender.Gossip(time.Now(), member.IP, gossipCmd, myDomain, ml.GetIncNumber(member.IP)); err != nil {
						log.Printf("Failed to send gossip to %s.\n", gMember.IP)
					}
				}
			}
		}

		time.Sleep(FD_period) // Wait before the next ping
	}
}

func joinFD(ml *MembershipList, domain string) {
	s := NewSender(IntroducerAddr, GossipPort, domain)
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
}
