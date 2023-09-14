package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/livepeer/go-tools/drivers"
)

func main() {
	err := cleanup(os.Getenv("url"), 7)
	if err != nil {
		log.Fatal(err)
	}
}

func cleanup(url string, ttlDays int) error {
	log.Println(url)
	osurl, err := drivers.ParseOSURL(url, true)
	if err != nil {
		return fmt.Errorf("failed to parse url %w", err)
	}
	session := osurl.NewSession("")

	fileCount := 0
	total := 0
	start := time.Now()
	pageinfo, err := session.ListFiles(context.Background(), "hls/", "")
	if err != nil {
		return fmt.Errorf("failed to list files %w", err)
	}
	log.Println("ListObjects() took ", time.Since(start))
	//log.Println(fileCount, pageinfo.Directories(), pageinfo.HasNextPage())

	for {
		total += len(pageinfo.Files())
		for _, file := range pageinfo.Files() {
			if file.LastModified.Before(time.Now().AddDate(0, 0, -ttlDays)) {
				log.Println("name:", path.Join(session.GetInfo().S3Info.Host, file.Name))
				fmt.Println("date:", file.LastModified.Format("2006-01-02"))
				fileCount++
				log.Println(fileCount)
				//err := session.DeleteFile(context.Background(), file.Name)
				//if err != nil {
				//	return err
				//}
			}
		}
		if !pageinfo.HasNextPage() {
			break
		}
		start := time.Now()
		pageinfo, err = pageinfo.NextPage()
		if err != nil {
			return fmt.Errorf("failed to get next page %w", err)
		}
		log.Println("ListObjects() took ", time.Since(start))
		time.Sleep(10 * time.Millisecond)
	}
	log.Println(fileCount)
	log.Println("total", total)
	return nil
}
