package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)

type TaskType int8

const (
	TaskTypeMap    TaskType = 0
	TaskTypeReduce TaskType = 1
)

type Task interface {
	Id() int
	Type() TaskType
}

type MapTask struct {
	TaskId        int
	TaskInputFile string
	NReduce       int
}

func NewMapTask(id int, inputFile string, nReduce int) *MapTask {
	return &MapTask{
		TaskId:        id,
		TaskInputFile: inputFile,
		NReduce:       nReduce,
	}
}

func (mt *MapTask) Id() int {
	return mt.TaskId
}

func (mt *MapTask) Type() TaskType {
	return TaskTypeMap
}

func (mt *MapTask) InputFile() string {
	return mt.TaskInputFile
}

func (mt *MapTask) Execute(mapf func(string, string) []KeyValue) error {
	content, err := ioutil.ReadFile(mt.InputFile())
	if err != nil {
		return err
	}
	keyValues := mapf(mt.InputFile(), string(content))
	intermediateFiles := map[int]*os.File{}
	for i := 0; i < mt.NReduce; i++ {
		file, err := ioutil.TempFile("./", "")
		if err != nil {
			return err
		}
		intermediateFiles[i] = file
	}
	for _, kv := range keyValues {
		file := intermediateFiles[ihash(kv.Key)%mt.NReduce]
		_, err = fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
		if err != nil {
			return err
		}
	}
	for i, file := range intermediateFiles {
		file.Close()
		err = os.Rename(file.Name(), fmt.Sprintf("mr-%v-%v", mt.TaskId, i))
		if err != nil {
			return err
		}
	}
	return nil
}

type ReduceTask struct {
	TaskId int
	NMap   int
}

func NewReduceTask(id, nMap int) *ReduceTask {
	return &ReduceTask{
		TaskId: id,
		NMap:   nMap,
	}
}

func (rt *ReduceTask) Id() int {
	return rt.TaskId
}

func (rt *ReduceTask) Type() TaskType {
	return TaskTypeReduce
}

func (rt *ReduceTask) IntermediateFiles() []string {
	var files []string
	for i := 0; i < rt.NMap; i++ {
		files = append(files, fmt.Sprintf("mr-%v-%v", i, rt.TaskId))
	}
	return files
}

func (rt *ReduceTask) Execute(reducef func(string, []string) string) error {
	var kvs KVs
	for _, file := range rt.IntermediateFiles() {
		intermediateFile, err := os.Open(file)
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(intermediateFile)
		for scanner.Scan() {
			line := scanner.Text()
			pieces := strings.Split(line, " ")
			kvs = append(kvs, KeyValue{Key: pieces[0], Value: pieces[1]})
		}
	}
	sort.Sort(kvs)
	outputFile, err := ioutil.TempFile("./", "")
	if err != nil {
		return err
	}
	i := 0
	for i < kvs.Len() {
		j := i + 1
		for j < kvs.Len() && kvs[j].Key == kvs[i].Key {
			j++
		}
		var values []string
		for _, kv := range kvs[i:j] {
			values = append(values, kv.Value)
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(outputFile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	outputFile.Close()
	err = os.Rename(outputFile.Name(), fmt.Sprintf("mr-out-%v", rt.TaskId))
	if err != nil {
		return err
	}
	return nil
}
