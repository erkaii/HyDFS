package main

import (
	"HyDFS/failuredetector"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
)

const (
	REP_NUM      = 3
	MAX_FILE_NUM = 1000
)

type File struct {
	filename string // Gives the path to local file on the server
}

type FileServer struct {
	aliveml   *failuredetector.MembershipList
	pred_list [REP_NUM]int
	files     [MAX_FILE_NUM]File
	id        int
}

func FileServerInit(ml *failuredetector.MembershipList, id int) *FileServer {
	return &FileServer{
		id:      id,
		files:   [MAX_FILE_NUM]File{},
		aliveml: ml,
	}
}

func (fs *FileServer) GetId() int {
	return fs.id
}

func HashKey(input string) int {
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
