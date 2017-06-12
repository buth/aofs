package aofs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestS3File(t *testing.T) {
	// https://dave.cheney.net/2017/06/11/go-without-package-scoped-variables
	var i interface{} = &S3File{}
	if _, ok := i.(File); !ok {
		t.Fatalf("expected %t to implement File", i)
	}
}

func TestS3FileSystem(t *testing.T) {
	// https://dave.cheney.net/2017/06/11/go-without-package-scoped-variables
	var i interface{} = &S3FileSystem{}
	if _, ok := i.(FileSystem); !ok {
		t.Fatalf("expected %t to implement FileSystem", i)
	}
}

func TestS3FileSystemWrite(t *testing.T) {
	awsSession, err := session.NewSession()
	if err != nil {
		t.Fatal(err)
	}

	bucketName := os.Getenv("AWS_BUCKET")
	bucket := aws.String(bucketName)
	keyName := "test"
	key := aws.String(keyName)
	deleteObjectInput := &s3.DeleteObjectInput{
		Bucket: bucket,
		Key:    key,
	}

	client := s3.New(awsSession)
	if _, err := client.DeleteObject(deleteObjectInput); err != nil {
		t.Fatal(err)
	}

	fs := NewS3FileSystem(S3FileSystemOptions{
		Bucket:  bucketName,
		Session: awsSession,
	})

	testFile, err := fs.Open(keyName)
	if err != nil {
		t.Fatal(err)
	}

	local := new(bytes.Buffer)
	writer := io.MultiWriter(local, testFile)
	for i := 0; i < 10; i++ {
		if _, err := fmt.Fprintf(writer, "%d\n", i); err != nil {
			t.Fatal(err)
		}
	}

	if err := testFile.Flush(); err != nil {
		t.Fatal(err)
	}

	for i := 10; i < 20; i++ {
		if _, err := fmt.Fprintf(writer, "%d\n", i); err != nil {
			t.Fatal(err)
		}
	}

	if err := testFile.Close(); err != nil {
		t.Fatal(err)
	}

	getObjectOutput, err := client.GetObject(&s3.GetObjectInput{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		t.Fatal(err)
	}

	remote, err := ioutil.ReadAll(getObjectOutput.Body)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(local.Bytes(), remote) {
		t.Errorf("local and remote files did not match\n%s\n%s\n", local.String(), string(remote))
	}
}
