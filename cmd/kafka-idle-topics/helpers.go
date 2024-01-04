package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

var Version = "1.1-SNAPSHOT"

type StringArrayFlag map[string]bool

func (i *StringArrayFlag) String() string {
	return fmt.Sprintln(*i)
}

func (i *StringArrayFlag) Set(value string) error {
	currentPath, _ := os.Getwd()

	if fileExists(value) {
		f, err := ioutil.ReadFile(CheckPath(value, currentPath))
		if err != nil {
			panic(err)
		}
		value = string(f)
	}

	nospaces := i.removeSpaces(value)

	tempMap := map[string]bool{}

	for _, s := range strings.Split(nospaces, ",") {
		tempMap[s] = true
	}

	*i = tempMap

	return nil
}

func (i *StringArrayFlag) removeSpaces(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}

func GetOSEnvVar(env_var string) (string, error) {
	key, present := os.LookupEnv(env_var)
	if present && key != "" {
		return key, nil
	}

	return "", errors.New("Environment variable has not been specified: " + env_var)
}

func isInSlice(i string, list []string) bool {
	for _, current := range list {
		if current == i {
			return true
		}
	}
	return false
}

func makeRange(min int32, max int32) []int32 {
	a := make([]int32, max-min+1)
	for i := range a {
		a[i] = min + int32(i)
	}
	return a
}

// Checks if the described path is a file
// It will return false if the file does not exist or the given is a directory
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// Returns a valid local FS path
func CheckPath(definedPath string, workingDirectory string) string {

	currentPath := filepath.Clean(workingDirectory)

	if definedPath == "" {
		definedPath = filepath.Join(currentPath, "SchemaRegistryBackup")
		log.Println("Path not defined, using local folder SchemaRegistryBackup")
		_ = os.Mkdir(definedPath, 0755)
		return definedPath
	} else {
		if filepath.IsAbs(definedPath) {
			if _, err := os.Stat(definedPath); os.IsNotExist(err) {
				log.Println("Path: " + definedPath)
				log.Fatalln("The directory specified does not exist.")
			}
		} else {
			definedPath = filepath.Join(currentPath, definedPath)
			_, err := os.Stat(definedPath)
			if os.IsNotExist(err) {
				log.Println("Path: " + definedPath)
				log.Fatalln("The directory specified does not exist.")
			}
		}
		return definedPath
	}
}
