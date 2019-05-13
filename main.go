package main

import (
	"fias/importing"
	"fmt"
	"github.com/mholt/archiver"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"time"
)

const lastDbfUrl = "http://fias.nalog.ru/Public/Downloads/Actual/fias_dbf.rar"

func main() {
	args := os.Args
	downloadFlag := "-download"
	unarchiveFlag := "-unarchive"
	importFlag := "-import"
	if len(args) < 3 || (len(args) == 4 && args[1] != unarchiveFlag) || len(args) > 4 ||
		(args[1] != downloadFlag && args[1] != unarchiveFlag && args[1] != importFlag) {
		fmt.Printf(`Usage:
	-download fileDirectory 		downloads fias rar file and saves it to fileDirectory
	-unarchive filePath dirToExtract 		unarchives rar file into dirToExtract
	-import fiasFilesDir 		imports dbf files from diasFilesDir
`)
		os.Exit(1)
	}
	if args[1] == downloadFlag {
		downloadLastDbf(args[2])
	} else if args[1] == unarchiveFlag {
		unarchiveDbf(args[2], args[3])
	} else {
		importRegions(args[2])
	}
}

func downloadLastDbf(fileDirectory string) {
	fmt.Printf("start rar file download\n")
	filePath := fileDirectory + "/fias.rar"
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("can't create file %s: %s\n", filePath, err)
	}
	defer file.Close()
	resp, err := http.Get(lastDbfUrl)
	if err != nil {
		log.Fatalf("cant load file %s\n", err)
	}
	defer resp.Body.Close()
	bytes, err := io.Copy(file, resp.Body)
	var sizeMB = int64(5 << (10 * 2))
	fmt.Printf("loaded file %s(%dMB)\n", filePath, bytes/sizeMB)
}

func unarchiveDbf(filePath string, dirToExtract string) {
	fmt.Printf("start unarchive %s to %s\n", filePath, dirToExtract)
	if _, err := os.Stat(dirToExtract); os.IsNotExist(err) {
		err := os.Mkdir(dirToExtract, os.ModePerm)
		if err != nil {
			log.Fatal(err)
		}
	}

	err := archiver.Unarchive(filePath, dirToExtract)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("unarchived %s", filePath)
}

func importRegions(fiasFilesDir string) {
	importer := importing.NewImporter()
	infos, err := ioutil.ReadDir(fiasFilesDir)
	if err != nil {
		log.Fatal("cannot read fias dir", err)
	}
	var codes []string
	for _, info := range infos {
		r := regexp.MustCompile("ADDROB(?P<code>[0-9]{2})\\.DBF")
		submatch := r.FindStringSubmatch(info.Name())
		subNames := r.SubexpNames()
		for idx, subName := range subNames {
			if subName == "code" && idx < len(submatch) && submatch[idx] != "" {
				codes = append(codes, submatch[idx])
			}
		}
	}
	importTime := time.Now()
	for _, regionCode := range codes {
		fmt.Printf("start loading region with code %s\n", regionCode)
		importer.Import(fiasFilesDir, regionCode)
		fmt.Printf("region %s loaded \n", regionCode)
	}
	importTimeDiff := time.Since(importTime)
	fmt.Printf("finished importing: %fmin\n", importTimeDiff.Minutes())
}
