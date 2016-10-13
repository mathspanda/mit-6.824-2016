package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvs := make(map[string][]string)
	keys := []string{}

	for i := 0; i < nMap; i++ {
		tempFileName := reduceName(jobName, i, reduceTaskNumber)
		tempFile, err := os.Open(tempFileName)
		if err != nil {
			return
		}

		dec := json.NewDecoder(tempFile)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}

			_, ok := kvs[kv.Key]
			if !ok {
				kvs[kv.Key] = []string{}
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}

		tempFile.Close()
	}

	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	mergeFileName := mergeName(jobName, reduceTaskNumber)
	mergeFile, err := os.Create(mergeFileName)
	defer mergeFile.Close()
	if err != nil {
		return
	}

	enc := json.NewEncoder(mergeFile)
	for _, key := range keys {
		res := reduceF(key, kvs[key])
		enc.Encode(KeyValue{key, res})
	}

	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
}
