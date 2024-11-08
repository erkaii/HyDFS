package main

import (
	"HyDFS/failuredetector"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	REP_NUM          = 3
	MAX_SERVER       = 10
	HTTP_PORT        = "4444"
	FILE_PATH_PREFIX = "../files/server/"
)

type File struct {
	filename string // Gives the path to local file on the server
}

type FileServer struct {
	aliveml            *failuredetector.MembershipList
	pred_list          [REP_NUM]int
	succ_list          [REP_NUM]int
	p_files            map[string]File
	r_files            map[string]File
	id                 int
	online             bool
	Mutex              sync.RWMutex
	coord_create_queue map[string]int
	coord_append_queue map[string]int
}

func FileServerInit(ml *failuredetector.MembershipList, id int) *FileServer {
	return &FileServer{
		id:                 id,
		p_files:            make(map[string]File),
		r_files:            make(map[string]File),
		aliveml:            ml,
		pred_list:          [REP_NUM]int{0},
		succ_list:          [REP_NUM]int{0},
		online:             false,
		coord_create_queue: make(map[string]int),
		coord_append_queue: make(map[string]int),
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

func equalSlices(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func findSuccessors(owner int, membershipList []int, n int) []int {
	var successors []int
	listLength := len(membershipList)

	// Find the index of the owner in the membership list
	ownerIndex := len(membershipList)
	for i, node := range membershipList {
		if node == owner {
			ownerIndex = i
			break
		}
	}

	// Iterate starting from the owner to find 'n' successors, wrapping around if necessary
	for i := 1; i <= n; i++ {
		successorIndex := (ownerIndex + i) % listLength
		successor := membershipList[successorIndex]
		successors = append(successors, successor)
	}

	return successors
}

func findPredecessors(owner int, membershipList []int, n int) []int {
	var predecessors []int
	listLength := len(membershipList)

	// Find the index of the owner in the membership list
	ownerIndex := len(membershipList)
	for i, node := range membershipList {
		if node == owner {
			ownerIndex = i
			break
		}
	}

	// Iterate starting from the owner to find 'n' successors, wrapping around if necessary
	for i := 1; i <= n; i++ {
		predecessorIndex := (ownerIndex + i) % listLength
		predecessor := membershipList[predecessorIndex]
		predecessors = append(predecessors, predecessor)
	}

	return predecessors
}

func findServerByfileID(ids []int, fileID int) int {
	server_id := -1
	min := 1000
	for _, i := range ids {
		if (i*100+1000-fileID)%1000 < min {
			server_id = i
			min = (i*100 + 1000 - fileID) % 1000
		}
	}
	return server_id
}

func fileExistsinPrimary(fs *FileServer, filename string) bool {
	fs.Mutex.Lock()
	defer fs.Mutex.Unlock()
	_, exists := fs.p_files[filename]
	if exists {
		return true
	} else {
		return false
	}
}

func fileExistsinReplica(fs *FileServer, filename string) bool {
	fs.Mutex.Lock()
	defer fs.Mutex.Unlock()
	_, exists := fs.r_files[filename]
	if exists {
		return true
	} else {
		return false
	}
}

func newComers(array1, array2 []int) []int {
	diff := []int{}
	elements := make(map[int]bool)

	// Add all elements of array1 to a map for quick lookups
	for _, num := range array1 {
		elements[num] = true
	}

	// Check elements in array2 and add to result if not in array1
	for _, num := range array2 {
		if !elements[num] {
			diff = append(diff, num)
		}
	}

	return diff
}

func id_to_domain(id int) string {
	return "fa24-cs425-68" + fmt.Sprintf("%02d", id) + ".cs.illinois.edu"
}

// Maintenance Thread
func Maintenance(fs *FileServer) {
	online := false

	for {
		// Update online=true only if all members are in the network.
		if !online && len(fs.aliveml.Alive_Ids()) == MAX_SERVER {
			fs.Mutex.Lock()
			fs.online = true
			fs.Mutex.Unlock()
		}

		if !fs.online {
			time.Sleep(time.Second)
			continue
		}

		//-------------- Maintenance logic ---------------//
		updatePredList(fs)
		updateSuccList(fs)

		// time.Sleep(time.Second)
	}
}

func updatePredList(fs *FileServer) {
	new_pred_list := findPredecessors(fs.id, fs.aliveml.Alive_Ids(), REP_NUM)
	fs.Mutex.Lock()
	old_pred_list := fs.pred_list
	fs.Mutex.Unlock()

	if equalSlices(new_pred_list, old_pred_list[:]) {
		return
	}

	fs.Mutex.Lock()
	fs.pred_list = [REP_NUM]int(new_pred_list)
	fs.Mutex.Unlock()

	// newPreds := newComers(old_pred_list[:], new_pred_list)

}

func updateSuccList(fs *FileServer) {
	new_succ_list := findSuccessors(fs.id, fs.aliveml.Alive_Ids(), REP_NUM)
	fs.Mutex.Lock()
	old_succ_list := fs.succ_list
	fs.Mutex.Unlock()

	if equalSlices(new_succ_list, old_succ_list[:]) {
		return
	}

	fs.Mutex.Lock()
	fs.succ_list = [REP_NUM]int(new_succ_list)
	fs.Mutex.Unlock()
}

// ------------------------- HTTP Handler -------------------------//
// Function to start HTTP server
func HTTPServer(fs *FileServer) {

	for {
		if fs.online {
			break
		}
	}

	http.HandleFunc("/", fs.httpHandleSlash)        // Handle slash request (used when client search coordinator servers)
	http.HandleFunc("/create", fs.httpHandleCreate) // Handle file creation requests
	http.HandleFunc("/creating", fs.httpHandleCreating)
	http.HandleFunc("/existfile", fs.httpHandleExistence)   // Handle file existence queries, return YES/NO
	http.HandleFunc("/membership", fs.httpHandleMembership) // Return ids of online servers
	http.HandleFunc("/online", fs.httpHandleOnline)         // Return YES/NO to indicate online/offline
	http.HandleFunc("/append", fs.httpHandleAppend)
	http.HandleFunc("/appending", fs.httpHandleAppending)
	http.HandleFunc("/get", fs.httpHandleGet)
	http.HandleFunc("/getting", fs.httpHandleGetting)
	http.HandleFunc("/store", fs.httpHandleStore)
	http.HandleFunc("/storedfilenames", fs.httpHandleStoredfilenames)

	fmt.Println("Starting HTTP server on :" + HTTP_PORT)
	log.Fatal(http.ListenAndServe(":"+HTTP_PORT, nil))
}

// HTTP handler functions
func (fs *FileServer) httpHandleCreate(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req map[string]string
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// Access file1 and file2 directly from the map
		_, localExists := req["local"]
		hydfs, hydfsExists := req["hydfs"]
		if !localExists || !hydfsExists {
			http.Error(w, "Missing localfilename or HyDFSfilename in request", http.StatusBadRequest)
			return
		}

		// Find out the primary server of the HyDFS file
		fileID := hashKey(hydfs)
		responsible_server_id := findServerByfileID(fs.aliveml.Alive_Ids(), fileID)
		if responsible_server_id == -1 {
			log.Println("Invalid findServerByfileID result in httpHandleCreate")
			http.Error(w, "Rejected due to server internal error", http.StatusBadRequest)
			return
		}

		// Check if allowed to create
		url := fmt.Sprintf("http://%s:%s/existfile?filename=%s&ftype=p", id_to_domain(responsible_server_id), HTTP_PORT, hydfs)
		req2, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			http.Error(w, "Failed when checking file existence", http.StatusInternalServerError)
			return
		}

		client := &http.Client{}
		resp, err := client.Do(req2)
		defer resp.Body.Close()

		existFlag := true
		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			if string(body) == "NO" {
				existFlag = false
			}
		}

		if !existFlag {
			// Write the request into a cache
			fs.Mutex.Lock()
			defer fs.Mutex.Unlock()

			fs.coord_create_queue[hydfs] = responsible_server_id

			fmt.Fprintf(w, "Authorized")
		} else {
			http.Error(w, "Rejected, file "+hydfs+" already exists", http.StatusBadRequest)
		}
		return
	case http.MethodPut:
		filename := r.URL.Query().Get("filename")
		if filename == "" {
			http.Error(w, "HyDFS Filename not specified", http.StatusBadRequest)
			return
		}

		// Read the file content from the request body
		fileContent, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read file content from request", http.StatusInternalServerError)
			return
		}

		fs.Mutex.Lock()
		responsible_server_id, exist := fs.coord_create_queue[filename]
		fs.Mutex.Unlock()

		if !exist {
			http.Error(w, "Invalid upload, file creation not allowed", http.StatusBadRequest)
			return
		}
		// Create a new request to the external server
		url := fmt.Sprintf("http://%s:%s/creating?filename=%s&ftype=p", id_to_domain(responsible_server_id), HTTP_PORT, filename)
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(fileContent))
		if err != nil {
			http.Error(w, "Failed to create request to external server", http.StatusInternalServerError)
			return
		}

		// Send the request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "Failed to send request to external server", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Check if the external server responded successfully
		if resp.StatusCode != http.StatusOK {
			http.Error(w, "External server error: "+resp.Status, resp.StatusCode)
			return
		}

		// Remove the task from queue
		fs.Mutex.Lock()
		delete(fs.coord_create_queue, filename)
		fs.Mutex.Unlock()
		fmt.Fprint(w, "File uploaded to external server "+id_to_domain(responsible_server_id)+" successfully")
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
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleExistence(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		filename := r.URL.Query().Get("filename")
		ftype := r.URL.Query().Get("ftype")

		if ftype == "p" {
			if fileExistsinPrimary(fs, filename) {
				w.Write([]byte("YES"))
			} else {
				w.Write([]byte("NO"))
			}
		} else {
			if fileExistsinReplica(fs, filename) {
				w.Write([]byte("YES"))
			} else {
				w.Write([]byte("NO"))
			}
		}

		return
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
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleAppending(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		// Get filename from query parameters
		filename := r.URL.Query().Get("filename")
		ftype := r.URL.Query().Get("ftype")
		if filename == "" {
			http.Error(w, "Filename not specified", http.StatusBadRequest)
			return
		}

		// Read the content from the request body
		content, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read content from request body", http.StatusInternalServerError)
			return
		}

		// Open the file in append mode, or create it if it doesn't exist
		file, err := os.OpenFile(FILE_PATH_PREFIX+filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			http.Error(w, "Failed to open or create file", http.StatusInternalServerError)
			return
		}
		defer file.Close()

		// Write the received content to the file
		_, err = file.Write(content)
		if err != nil {
			http.Error(w, "Failed to write content to file", http.StatusInternalServerError)
			return
		}

		// Respond to confirm the operation was successful
		fs.Mutex.Lock()
		if ftype == "p" {
			fs.p_files[filename] = File{filename: filename}
		} else {
			fs.r_files[filename] = File{filename: filename}
		}
		fs.Mutex.Unlock()

		// Pushing changes to replicas
		if ftype == "p" {
			fs.Mutex.Lock()
			succ_list_temp := fs.succ_list
			fs.Mutex.Unlock()
			for _, i := range succ_list_temp {
				// Create a new request to the external server
				url := fmt.Sprintf("http://%s:%s/appending?filename=%s&ftype=r", id_to_domain(i), HTTP_PORT, filename)
				req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(content))
				if err != nil {
					http.Error(w, "Failed to create request to external server", http.StatusInternalServerError)
					return
				}

				// Send the request
				client := &http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					http.Error(w, "Failed to send request to external server", http.StatusInternalServerError)
					return
				}
				defer resp.Body.Close()
			}
		}

		fmt.Fprint(w, "File content appended successfully")
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleCreating(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		// Get filename from query parameters
		filename := r.URL.Query().Get("filename")
		ftype := r.URL.Query().Get("ftype")
		if filename == "" {
			http.Error(w, "Filename not specified", http.StatusBadRequest)
			return
		}

		// Read the content from the request body
		content, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read content from request body", http.StatusInternalServerError)
			return
		}

		// Open the file in append mode, or create it if it doesn't exist
		file, err := os.OpenFile(FILE_PATH_PREFIX+filename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			http.Error(w, "Failed to open or create file", http.StatusInternalServerError)
			return
		}
		defer file.Close()

		// Write the received content to the file
		_, err = file.Write(content)
		if err != nil {
			http.Error(w, "Failed to write content to file", http.StatusInternalServerError)
			return
		}

		fs.Mutex.Lock()
		if ftype == "p" {
			fs.p_files[filename] = File{filename: filename}
		} else {
			fs.r_files[filename] = File{filename: filename}
		}
		fs.Mutex.Unlock()

		// Pushing create to replicas
		if ftype == "p" {
			fs.Mutex.Lock()
			succ_list_temp := fs.succ_list
			fs.Mutex.Unlock()
			for _, i := range succ_list_temp {
				// Create a new request to the external server
				url := fmt.Sprintf("http://%s:%s/creating?filename=%s&ftype=r", id_to_domain(i), HTTP_PORT, filename)
				req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(content))
				if err != nil {
					http.Error(w, "Failed to create request to external server", http.StatusInternalServerError)
					return
				}

				// Send the request
				client := &http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					http.Error(w, "Failed to send request to external server", http.StatusInternalServerError)
					return
				}
				defer resp.Body.Close()
			}
		}

		fmt.Fprint(w, "File content created successfully")
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleGet(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		var req map[string]string
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// Access file1 and file2 directly from the map
		_, localExists := req["local"]
		hydfs, hydfsExists := req["hydfs"]
		if !localExists || !hydfsExists {
			http.Error(w, "Missing localfilename or HyDFSfilename in request", http.StatusBadRequest)
			return
		}

		// Find out the primary server of the HyDFS file
		fileID := hashKey(hydfs)
		responsible_server_id := findServerByfileID(fs.aliveml.Alive_Ids(), fileID)
		if responsible_server_id == -1 {
			log.Println("Invalid findServerByfileID result in httpHandleCreate")
			http.Error(w, "Rejected due to server internal error", http.StatusBadRequest)
			return
		}

		url := fmt.Sprintf("http://%s:%s/getting?filename=%s&ftype=p", id_to_domain(responsible_server_id), HTTP_PORT, hydfs)
		req2, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			http.Error(w, "Failed when checking file existence", http.StatusInternalServerError)
			return
		}

		client := &http.Client{}
		resp, err := client.Do(req2)
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			http.Error(w, "Rejected, file "+hydfs+" doesn't exist", http.StatusBadRequest)
			return
		}

		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(w, "%s", string(body))

		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleGetting(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		filename := r.URL.Query().Get("filename")
		ftype := r.URL.Query().Get("ftype")

		exist_flag := false
		fs.Mutex.Lock()
		if ftype == "p" {
			_, exist_flag = fs.p_files[filename]
		} else {
			_, exist_flag = fs.r_files[filename]
		}
		fs.Mutex.Unlock()

		if !exist_flag {
			http.Error(w, "Rejected, file "+filename+" doesn't exist", http.StatusBadRequest)
		}

		// Attempt to read the file content
		fileContent, err := os.ReadFile(FILE_PATH_PREFIX + filename)
		if err != nil {
			http.Error(w, "Could not read file: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Send the file content as the HTTP response
		w.WriteHeader(http.StatusOK)
		w.Write(fileContent)

		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleAppend(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req map[string]string
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// Access file1 and file2 directly from the map
		_, localExists := req["local"]
		hydfs, hydfsExists := req["hydfs"]
		if !localExists || !hydfsExists {
			http.Error(w, "Missing localfilename or HyDFSfilename in request", http.StatusBadRequest)
			return
		}

		// Find out the primary server of the HyDFS file
		fileID := hashKey(hydfs)
		responsible_server_id := findServerByfileID(fs.aliveml.Alive_Ids(), fileID)
		if responsible_server_id == -1 {
			log.Println("Invalid findServerByfileID result in httpHandleAppend")
			http.Error(w, "Rejected due to server internal error", http.StatusBadRequest)
			return
		}

		// Check if allowed to append
		url := fmt.Sprintf("http://%s:%s/existfile?filename=%s&ftype=p", id_to_domain(responsible_server_id), HTTP_PORT, hydfs)
		req2, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			http.Error(w, "Failed when checking file existence", http.StatusInternalServerError)
			return
		}

		client := &http.Client{}
		resp, err := client.Do(req2)
		defer resp.Body.Close()

		existFlag := true
		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			if string(body) == "NO" {
				existFlag = false
			}
		}

		if existFlag {
			// Write the request into a cache
			fs.Mutex.Lock()
			defer fs.Mutex.Unlock()

			fs.coord_append_queue[hydfs] = responsible_server_id

			fmt.Fprintf(w, "Authorized")
		} else {
			http.Error(w, "Rejected, file "+hydfs+" doesn't exist", http.StatusBadRequest)
		}
		return
	case http.MethodPut:
		filename := r.URL.Query().Get("filename")
		if filename == "" {
			http.Error(w, "HyDFS Filename not specified", http.StatusBadRequest)
			return
		}

		// Read the file content from the request body
		fileContent, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read file content from request", http.StatusInternalServerError)
			return
		}

		fs.Mutex.Lock()
		responsible_server_id, exist := fs.coord_append_queue[filename]
		fs.Mutex.Unlock()

		if !exist {
			http.Error(w, "Invalid upload, file creation not allowed", http.StatusBadRequest)
			return
		}
		// Create a new request to the external server
		url := fmt.Sprintf("http://%s:%s/appending?filename=%s&ftype=p", id_to_domain(responsible_server_id), HTTP_PORT, filename)
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(fileContent))
		if err != nil {
			http.Error(w, "Failed to create request to external server", http.StatusInternalServerError)
			return
		}

		// Send the request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "Failed to send request to external server", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Check if the external server responded successfully
		if resp.StatusCode != http.StatusOK {
			http.Error(w, "External server error: "+resp.Status, resp.StatusCode)
			return
		}

		// Remove the task from queue
		fs.Mutex.Lock()
		delete(fs.coord_append_queue, filename)
		fs.Mutex.Unlock()
		fmt.Fprint(w, "File uploaded to external server "+id_to_domain(responsible_server_id)+" successfully")
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleStoredfilenames(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ftype := r.URL.Query().Get("ftype")

		var file_list map[string]File
		fs.Mutex.Lock()
		if ftype == "p" {
			file_list = fs.p_files
		} else {
			file_list = fs.r_files
		}
		fs.Mutex.Unlock()

		keys := make([]string, 0, len(file_list))
		for key := range file_list {
			keys = append(keys, key)
		}

		filenameString := strings.Join(keys, " ")

		w.Write([]byte(filenameString))

		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleStore(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		fs.Mutex.Lock()
		alive_ids := fs.aliveml.Alive_Ids()
		fs.Mutex.Unlock()

		response_string := ""

		for _, i := range alive_ids {
			response_string += "vm id " + strconv.Itoa(i) + ":\n"

			url := fmt.Sprintf("http://%s:%s/storedfilenames?ftype=p", id_to_domain(i), HTTP_PORT)
			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				http.Error(w, "Failed when getting filenames", http.StatusInternalServerError)
				return
			}
			client := &http.Client{}
			resp, err := client.Do(req)
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			response_string += "primaries: " + string(body) + "\n"

			url2 := fmt.Sprintf("http://%s:%s/storedfilenames?ftype=r", id_to_domain(i), HTTP_PORT)
			req2, err := http.NewRequest(http.MethodGet, url2, nil)
			if err != nil {
				http.Error(w, "Failed when getting filenames", http.StatusInternalServerError)
				return
			}
			client2 := &http.Client{}
			resp2, err := client2.Do(req2)
			defer resp2.Body.Close()

			body2, _ := io.ReadAll(resp2.Body)
			response_string += "replicas: " + string(body2) + "\n"
		}

		w.Write([]byte(response_string))

		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
