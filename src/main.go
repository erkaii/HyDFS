package main

import (
	"HyDFS/failuredetector"
	"fmt"
)

func main() {
	fmt.Println(HashKey("xyz"))
	failuredetector.Failuredetect()
}
