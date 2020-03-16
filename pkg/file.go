package pkg

import "os"

func Exist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}
