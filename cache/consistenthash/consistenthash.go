package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// 一致性哈希算法将 key 映射到 2^32 的空间中，将这个数字首尾相连，形成一个环。
// 计算节点/机器(通常使用节点的名称、编号和 IP 地址)的哈希值，放置在环上。
// 计算 key 的哈希值，放置在环上，顺时针寻找到的第一个节点，就是应选取的节点/机器。

// 解决数据倾斜问题：
// 第一步，计算虚拟节点的 Hash 值，放置在环上。
// 第二步，计算 key 的 Hash 值，在环上顺时针寻找到应选取的虚拟节点，例如是 peer2-1，
// 那么就对应真实节点 peer2。

type Hash func(data []byte) uint32

// Map 是一致性哈希算法的主数据结构，包含 4 个成员变量：Hash 函数 hash；
// 虚拟节点倍数 replicas；哈希环 keys；虚拟节点与真实节点的映射表 hashMap，
// 键是虚拟节点的哈希值，值是真实节点的名称。
type Map struct {
	hash       Hash
	replicates int
	keys       []int
	hashMap    map[int]string
}

func New(replicats int, fn Hash) *Map {
	m := &Map{
		replicates: replicats,
		hash:       fn,
		hashMap:    make(map[int]string),
	}
	if m.hash == nil {
		// 返回crc-32校验 使用IEEE多项式 默认算法
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add 函数允许传入 0 或 多个真实节点的名称。
// 对每一个真实节点 key，对应创建 m.replicas 个虚拟节点，
// 虚拟节点的名称是：strconv.Itoa(i) + key，即通过添加编号的方式区分不同虚拟节点。
// 使用 m.hash() 计算虚拟节点的哈希值，使用 append(m.keys, hash) 添加到环上。
// 在 hashMap 中增加虚拟节点和真实节点的映射关系。
// 最后一步，环上的哈希值排序。
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicates; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	return m.hashMap[m.keys[idx%len(m.keys)]]
}
