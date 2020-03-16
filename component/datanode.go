package component

type AppendOnlyQueue interface {
}

/*

|  DELETED  ||       IN STORAGE       |
| expired * || sent * || to be sent * |
          ↑         ↑               ↑
expiredIndex  sentIndex        lastIndex

 */

type appendOnlyQueue struct {
	lastIndex   uint64 // last index of entries
	sentIndex   uint64 // last index of sent entries
	expiredIndex uint64 // last index of expired entries
}
