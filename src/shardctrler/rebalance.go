package shardctrler

import (
	"container/heap"
	"sort"
)

type AssignEntry struct {
	GID    int
	Shards []int
}

type MaxHeap []AssignEntry

func (maxHeap MaxHeap) Len() int {
	return len(maxHeap)
}

func (maxHeap MaxHeap) Less(i, j int) bool {
	if len(maxHeap[i].Shards) != len(maxHeap[j].Shards) {
		return len(maxHeap[i].Shards) > len(maxHeap[j].Shards)
	}
	return maxHeap[i].GID < maxHeap[j].GID
}

func (maxHeap MaxHeap) Swap(i, j int) {
	maxHeap[i], maxHeap[j] = maxHeap[j], maxHeap[i]
}

func (maxHeap *MaxHeap) Push(x interface{}) {
	*maxHeap = append(*maxHeap, x.(AssignEntry))
}

func (maxHeap *MaxHeap) Pop() interface{} {
	ret := (*maxHeap)[len(*maxHeap)-1]
	*maxHeap = (*maxHeap)[:len(*maxHeap)-1]
	return ret
}

type MinHeap []AssignEntry

func (minHeap MinHeap) Len() int {
	return len(minHeap)
}

func (minHeap MinHeap) Less(i, j int) bool {
	if len(minHeap[i].Shards) != len(minHeap[j].Shards) {
		return len(minHeap[i].Shards) < len(minHeap[j].Shards)
	}
	return minHeap[i].GID < minHeap[j].GID
}

func (minHeap MinHeap) Swap(i, j int) {
	minHeap[i], minHeap[j] = minHeap[j], minHeap[i]
}

func (minHeap *MinHeap) Push(x interface{}) {
	*minHeap = append(*minHeap, x.(AssignEntry))
}

func (minHeap *MinHeap) Pop() interface{} {
	ret := (*minHeap)[len(*minHeap)-1]
	*minHeap = (*minHeap)[:len(*minHeap)-1]
	return ret
}

func (sc *ShardCtrler) join(servers map[int][]string) {
	newConfig := sc.configs[len(sc.configs)-1].Copy()
	newConfig.Num++
	mp := map[int][]int{}
	for shard, gid := range newConfig.Shards {
		mp[gid] = append(mp[gid], shard)
	}
	hp := &MaxHeap{}
	for gid, shards := range mp {
		hp.Push(AssignEntry{GID: gid, Shards: shards})
	}
	heap.Init(hp)
	var GIDs []int
	for gid := range servers {
		GIDs = append(GIDs, gid)
	}
	sort.Ints(GIDs)
	for _, gid := range GIDs {
		if newConfig.Groups[gid] != nil {
			continue
		}
		assignNum := len(newConfig.Shards) / (len(newConfig.Groups) + 1)
		assignEntry := AssignEntry{GID: gid, Shards: []int{}}
		for i := 0; i < assignNum; i++ {
			entry := heap.Pop(hp).(AssignEntry)
			newConfig.Shards[entry.Shards[0]] = gid
			assignEntry.Shards = append(assignEntry.Shards, entry.Shards[0])
			entry.Shards = entry.Shards[1:]
			heap.Push(hp, entry)
		}
		heap.Push(hp, assignEntry)
		newConfig.Groups[gid] = servers[gid]
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) leave(GIDs []int) {
	newConfig := sc.configs[len(sc.configs)-1].Copy()
	newConfig.Num++
	for _, gid := range GIDs {
		newConfig.Groups[gid] = nil
		delete(newConfig.Groups, gid)
	}
	hp := &MinHeap{}
	for gid := range newConfig.Groups {
		assignEntry := AssignEntry{GID: gid, Shards: []int{}}
		for shard, assignGID := range newConfig.Shards {
			if assignGID == gid {
				assignEntry.Shards = append(assignEntry.Shards, shard)
			}
		}
		hp.Push(assignEntry)
	}
	heap.Init(hp)
	for _, leaveGID := range GIDs {
		for shard, assignGID := range newConfig.Shards {
			if assignGID == leaveGID {
				if hp.Len() == 0 {
					newConfig.Shards[shard] = 0
				} else {
					assignEntry := heap.Pop(hp).(AssignEntry)
					newConfig.Shards[shard] = assignEntry.GID
					assignEntry.Shards = append(assignEntry.Shards, shard)
					heap.Push(hp, assignEntry)
				}
			}
		}
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) move(shard int, GID int) {
	newConfig := sc.configs[len(sc.configs)-1].Copy()
	newConfig.Num++
	newConfig.Shards[shard] = GID
	sc.configs = append(sc.configs, newConfig)
}
