package main

import (
	"bufio"
	"compress/bzip2"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"log"
	"os"
)

type Article struct {
	Title      string
	ArticleId  string
	RevisionId string
	Text       string
}

func main() {
	flagDumpPath := flag.String("dump", "", "Input path of wikipedia(en) dump files (enwiki-*-pages-articles)")
	flag.Parse()

	file, err := os.Open(*flagDumpPath)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("I'm mighty treebeard", *flagDumpPath)

	// mediawiki > page > (title + (revision > text))
	wikiXml := xml.NewDecoder(bzip2.NewReader(file))
	inTitle := false
	inRevision := false
	inId := false
	inText := false

	countWritten := 0
	countTotal := 0
	const nShard = 20
	currentPage := Article{}
	shardFiles := []*bufio.Writer{}
	for shardIndex := 0; shardIndex < nShard; shardIndex++ {
		shardPath := fmt.Sprintf("/data/treebeard-temp/articles-%02d.json", shardIndex)
		f, err := os.Create(shardPath)
		defer f.Close()
		if err != nil {
			log.Fatal(err)
		}
		shardFiles = append(shardFiles, bufio.NewWriter(f))
	}
	for {
		token, err := wikiXml.Token()
		if err != nil {
			break
		}

		switch s := token.(type) {
		case xml.StartElement:
			if s.Name.Local == "page" {
				currentPage = Article{}
			} else if s.Name.Local == "title" {
				inTitle = true
			} else if s.Name.Local == "revision" {
				inRevision = true
			} else if s.Name.Local == "id" {
				inId = true
			} else if s.Name.Local == "text" {
				inText = true
			}
		case xml.EndElement:
			if s.Name.Local == "page" {
				if countTotal%1000 == 0 {
					log.Printf("Current countWritten=%d countTotal=%d\n", countWritten, countTotal)
				}
				js, err := json.Marshal(currentPage)
				if err != nil {
					log.Println("Ignoring article because", err)
				} else {
					shardFile := shardFiles[countWritten%nShard]
					shardFile.Write(js)
					shardFile.WriteRune('\n')
					countWritten++
				}
				countTotal++
			} else if s.Name.Local == "title" {
				inTitle = false
			} else if s.Name.Local == "revision" {
				inRevision = false
			} else if s.Name.Local == "id" {
				inId = false
			} else if s.Name.Local == "text" {
				inText = false
			}
		case xml.CharData:
			if inTitle {
				currentPage.Title = string(s)
			} else if inText {
				currentPage.Text = string(s)
			} else if inId && inRevision {
				currentPage.RevisionId = string(s)
			} else if inId {
				currentPage.ArticleId = string(s)
			}
		}
	}
	for _, shard := range shardFiles {
		shard.Flush()
	}
	fmt.Printf("Finished // countWritten:%d countTotal:%d\n", countWritten, countTotal)
}
