package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	doneChannel := make(chan int, ntasks)

	for i := 0; i < ntasks; i++ {
		go func(taskNo int, nios int, phase jobPhase) {
			for {
				worker := <-mr.registerChannel
				args := &DoTaskArgs{
					JobName:       mr.jobName,
					File:          mr.files[taskNo],
					Phase:         phase,
					TaskNumber:    taskNo,
					NumOtherPhase: nios,
				}

				ok := call(worker, "Worker.DoTask", args, nil)
				if ok {
					doneChannel <- taskNo
					mr.registerChannel <- worker
					break
				}
			}
		}(i, nios, phase)
	}

	for i := 0; i < ntasks; i++ {
		<-doneChannel
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
