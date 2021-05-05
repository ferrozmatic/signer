package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	var in, out chan interface{}
	wg := &sync.WaitGroup{}

	for _, myJob := range jobs {
		wg.Add(1)
		out = make(chan interface{}, 100)

		go func(myJob job, in, out chan interface{}, wg *sync.WaitGroup) {
			myJob(in, out)

			wg.Done()
			close(out)
		}(myJob, in, out, wg)

		in = out
	}

	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	for data := range in {
		wg.Add(1)

		go func(data interface{}, wg *sync.WaitGroup, mu *sync.Mutex) {
			convertedData := fmt.Sprintf("%v", data)
			crc32DataChan := make(chan interface{})
			crc32Md5DataChan := make(chan interface{})

			go func(convertedData string) {
				crc32DataChan <- DataSignerCrc32(convertedData)
			}(convertedData)

			go func(convertedData string, mu *sync.Mutex) {
				mu.Lock()
				md5Data := DataSignerMd5(convertedData)
				mu.Unlock()

				crc32Md5DataChan <- DataSignerCrc32(md5Data)
			}(convertedData, mu)

			out <- fmt.Sprintf("%s~%s", <-crc32DataChan, <-crc32Md5DataChan)
			wg.Done()
		}(data, wg, mu)
	}

	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for data := range in {
		wg.Add(1)

		go func(data interface{}, wg *sync.WaitGroup) {
			results := make([]string, 6)
			resultsWg := &sync.WaitGroup{}
			resultsMu := &sync.Mutex{}

			for index := 0; index < 6; index++ {
				resultsWg.Add(1)

				go func(results []string, index int, convertedData interface{}, resultsWg *sync.WaitGroup, resultsMu *sync.Mutex) {
					result := DataSignerCrc32(fmt.Sprintf("%d%s", index, convertedData))

					resultsMu.Lock()
					results[index] = result
					resultsMu.Unlock()

					resultsWg.Done()
				}(results, index, fmt.Sprintf("%v", data), resultsWg, resultsMu)
			}

			resultsWg.Wait()

			out <- strings.Join(results, "")
			wg.Done()
		}(data, wg)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var results []string

	for data := range in {
		results = append(results, fmt.Sprintf("%v", data))
	}

	sort.Strings(results)
	out <- strings.Join(results, "_")
}
