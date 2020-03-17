package pkg

import (
	"io/ioutil"
	"os"
)

func Exist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func Write(path string, data []byte) error {
	return ioutil.WriteFile(path, data, os.ModeAppend)
}
