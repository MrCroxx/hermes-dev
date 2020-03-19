package component

import (
	"errors"
	"fmt"
	"io/ioutil"
	"mrcroxx.io/hermes/log"
	"mrcroxx.io/hermes/pkg"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

var (
	errOutOfRange = errors.New("index out of range")
)

type DataStore interface {
	Get(n uint64) ([]string, uint64)
	Append(vs []string) uint64
	Cache(n uint64) error
	Persist(n uint64) error
	LenFresh() uint64
	LenCached() uint64
	LenPersisted() uint64
	CompactStorage(n uint64)
	GetSnapshot() ([]byte, error)
	RecoverFromSnapshot([]byte) error
	Indexes() (deletedIndex uint64, persistedIndex uint64, cachedIndex uint64, freshIndex uint64)
}

type dataStore struct {
	DeletedIndex   uint64
	PersistedIndex uint64
	CachedIndex    uint64
	FreshIndex     uint64

	StorageDir string

	CachedData []string
	FreshData  []string
}

func NewDataStore(storageDir string) DataStore {
	ds := &dataStore{
		DeletedIndex:   0,
		PersistedIndex: 0,
		CachedIndex:    0,
		FreshIndex:     0,
		CachedData:     make([]string, 0),
		FreshData:      make([]string, 0),
		StorageDir:     storageDir,
	}
	return ds
}

func (ds *dataStore) Get(n uint64) ([]string, uint64) {
	nfresh := uint64(len(ds.FreshData))
	if nfresh < n {
		n = nfresh
	}
	result := ds.FreshData[:n]
	return result, n
}

func (ds *dataStore) Append(vs []string) uint64 {
	ds.FreshData = append(ds.FreshData, vs...)
	ds.FreshIndex += uint64(len(vs))
	return uint64(len(vs))
}

func (ds *dataStore) Cache(n uint64) error {
	if uint64(len(ds.FreshData)) < n {
		return errOutOfRange
	}
	ds.CachedData = append(ds.CachedData, ds.FreshData[:n]...)
	ds.FreshData = ds.FreshData[n:]
	ds.CachedIndex += n
	return nil
}

func (ds *dataStore) Persist(n uint64) error {
	if uint64(len(ds.CachedData)) < n {
		return errOutOfRange
	}
	data, err := pkg.Encode(ds.CachedData[:n])
	if err != nil {
		return err
	}
	err = pkg.Write(path.Join(ds.StorageDir, fmt.Sprintf("%d-%d", ds.PersistedIndex, ds.PersistedIndex+n)), data)
	if err != nil {
		return err
	}
	ds.CachedData = ds.CachedData[n:]
	ds.PersistedIndex += n
	return nil
}

func (ds *dataStore) LenFresh() uint64 { return ds.FreshIndex - ds.CachedIndex }

func (ds *dataStore) LenCached() uint64 { return ds.CachedIndex - ds.PersistedIndex }

func (ds *dataStore) LenPersisted() uint64 {
	fs, err := ioutil.ReadDir(ds.StorageDir)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised reading data node storage dir, err=%s.", err)
		return 0
	}
	s := uint64(0)
	t := uint64(0)
	for _, f := range fs {
		if f.IsDir() {
			continue
		}
		ss := strings.Split(f.Name(), "-")
		if len(ss) != 2 {
			continue
		}
		x, err := strconv.ParseUint(ss[0], 10, 64)
		if err != nil {
			continue
		}
		y, err := strconv.ParseUint(ss[1], 10, 64)
		if err != nil {
			continue
		}
		if x < s {
			s = x
		}
		if y > t {
			y = t
		}
	}
	return t - s
}

func (ds *dataStore) Indexes() (deletedIndex uint64, persistedIndex uint64, cachedIndex uint64, freshIndex uint64) {
	return ds.DeletedIndex, ds.PersistedIndex, ds.CachedIndex, ds.FreshIndex
}

func (ds *dataStore) CompactStorage(n uint64) {
	type st struct {
		s, t uint64
	}
	fs, err := ioutil.ReadDir(ds.StorageDir)
	if err != nil {
		log.ZAPSugaredLogger().Errorf("Error raised reading data node storage dir, err=%s.", err)
		return
	}
	sts := []st{}
	for _, f := range fs {
		if f.IsDir() {
			continue
		}
		ss := strings.Split(f.Name(), "-")
		if len(ss) != 2 {
			continue
		}
		x, err := strconv.ParseUint(ss[0], 10, 64)
		if err != nil {
			continue
		}
		y, err := strconv.ParseUint(ss[1], 10, 64)
		if err != nil {
			continue
		}
		sts = append(sts, st{s: x, t: y})
	}
	sort.Slice(sts, func(i, j int) bool {
		return sts[i].t < sts[j].t
	})
	remain := uint64(0)
	i := len(sts) - 1
	for ; i >= 0; i-- {
		if remain+(sts[i].t-sts[i].s) > n {
			break
		}
	}
	for ; i >= 0; i-- {
		err := os.Remove(path.Join(ds.StorageDir, fmt.Sprintf("%d-%d", sts[i].s, sts[i].t)))
		if err != nil {
			log.ZAPSugaredLogger().Errorf("Error raised when deleting node storage file, err=%s.", err)
		}
	}
}

func (ds *dataStore) GetSnapshot() ([]byte, error) {
	return pkg.Encode(ds)
}

func (ds *dataStore) RecoverFromSnapshot(snap []byte) error {
	return pkg.Decode(snap, ds)
}
