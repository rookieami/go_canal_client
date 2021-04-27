package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var wg =sync.WaitGroup{}
func TestGO(T *testing.T) {

	tasks := []string{"1", "2", "3", "4", "5"}

	for _, ta := range tasks{
		wg.Add(1)
		go func(tl string) {
			defer wg.Done()
			if tl=="3" {
				time.Sleep(2.*time.Second)
			}
			fmt.Println("------",tl)

		}(ta)
	}
	//time.Sleep(5*time.Second)
	wg.Wait()
}
