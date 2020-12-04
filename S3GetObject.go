package main

import (
	"bytes"
	"fmt"
	//"reflect"
	"bufio"
	//"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	bucket := "d-s3-quick-cyq-lsb" // バケット名
	path := "fargate/1.log"        // ファイルパス

	svc := s3.New(session.New(), &aws.Config{
		Region: aws.String("ap-northeast-1"),
	})

	obj, _ := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	})
	defer obj.Body.Close()

	buf := new(bytes.Buffer)
	buf.ReadFrom(obj.Body)

	var lines []string

	scanner := bufio.NewScanner(buf)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	fmt.Println(lines)

	fmt.Println(len(lines))

}
