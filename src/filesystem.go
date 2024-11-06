package main

import (
	"HyDFS/failuredetector"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	REP_NUM    = 3
	MAX_SERVER = 10
	HTTP_PORT  = "4444"
)

type File struct {
	filename string // Gives the path to local file on the server
}

type FileServer struct {
	aliveml   *failuredetector.MembershipList
	pred_list [REP_NUM]int
	succ_list [REP_NUM]int
	p_files   map[string]File
	r_files   map[string]File
	id        int
	online    bool
	Mutex     sync.RWMutex
}

func FileServerInit(ml *failuredetector.MembershipList, id int) *FileServer {
	return &FileServer{
		id:        id,
		p_files:   make(map[string]File),
		r_files:   make(map[string]File),
		aliveml:   ml,
		pred_list: [REP_NUM]int{0},
		succ_list: [REP_NUM]int{0},
		online:    false,
	}
}

func hashKey(input string) int {
	// Create a new SHA256 hash
	hash := sha256.New()
	// Write the input string as bytes to the hash
	hash.Write([]byte(input))
	// Get the resulting hash as a byte slice
	hashedBytes := hash.Sum(nil)
	// Convert the hash bytes to a hexadecimal string
	hashString := hex.EncodeToString(hashedBytes)

	// Convert the hex string to a big.Int
	bigIntHash := new(big.Int)
	bigIntHash.SetString(hashString, 16)

	// Mod the big.Int by 1000 and add 1 to map to the range [1, 1000]
	result := bigIntHash.Mod(bigIntHash, big.NewInt(1000)).Int64() + 1

	return int(result)
}

func id_to_domain(id int) string {
	return "fa24-cs425-68" + fmt.Sprintf("%02d", id) + ".cs.illinois.edu"
}

// Maintenance Thread
func Maintenance(fs *FileServer) {
	for {
		fs.Mutex.Lock()
		if len(fs.aliveml.Alive_Ids()) == MAX_SERVER {
			fs.online = true
		}
		if !fs.online {
			fs.Mutex.Unlock()
			time.Sleep(time.Second)
			continue
		}
		fs.Mutex.Unlock()

		time.Sleep(time.Second)
	}
}

// ------------------------- HTTP Handler -------------------------//
// Function to start HTTP server
func HTTPServer(fs *FileServer) {

	http.HandleFunc("/", fs.httpHandleSlash)
	http.HandleFunc("/create", fs.httpHandleCreate)
	http.HandleFunc("/membership", fs.httpHandleMembership)
	http.HandleFunc("/online", fs.httpHandleOnline)

	fmt.Println("Starting HTTP server on :" + HTTP_PORT)
	log.Fatal(http.ListenAndServe(":"+HTTP_PORT, nil))
}

// HTTP handler functions
func (fs *FileServer) httpHandleCreate(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	case http.MethodPost:
		var req map[string]string
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// Access file1 and file2 directly from the map
		local, localExists := req["local"]
		hydfs, hydfsExists := req["hydfs"]
		if !localExists || !hydfsExists {
			http.Error(w, "Missing localfilename or HyDFSfilename in request", http.StatusBadRequest)
			return
		}

		fs.Mutex.Lock()
		defer fs.Mutex.Unlock()

		// Send response
		fmt.Fprintf(w, "File %s created as %s successfully", local, hydfs)
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleSlash(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Write([]byte{})
		return
	case http.MethodPost:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleMembership(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ids := fs.aliveml.Alive_Ids()
		message := "No member in the file system??"
		if len(ids) != 0 {
			strs := make([]string, len(ids))
			for i, num := range ids {
				strs[i] = strconv.Itoa(num) // Convert each int to string
			}
			message = strings.Join(strs, ", ")
		}
		w.Write([]byte(message))
		return
	case http.MethodPost:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleOnline(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		fs.Mutex.Lock()
		defer fs.Mutex.Unlock()
		if fs.online {
			w.Write([]byte("Yes"))
		} else {
			w.Write([]byte("No"))
		}
		return
	case http.MethodPost:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
