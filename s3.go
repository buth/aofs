package aofs

import (
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hashicorp/go-multierror"
)

type S3File struct {
	written  int
	bucket   *string
	key      *string
	cache    *os.File
	uploader *s3manager.Uploader
}

func (f *S3File) Write(p []byte) (n int, err error) {
	n, err = f.cache.Write(p)
	f.written += n
	return
}

func (f *S3File) Flush() error {
	if f.written == 0 {
		return nil
	}

	if _, err := f.cache.Seek(0, 0); err != nil {
		return err
	}
	_, err := f.uploader.Upload(&s3manager.UploadInput{
		Bucket: f.bucket,
		Key:    f.key,
		Body:   f.cache,
	})

	if _, err := f.cache.Seek(0, 2); err != nil {
		return err
	}

	f.written = 0
	return err
}

func (f *S3File) Close() error {
	err := f.Flush()
	if cerr := f.cache.Close(); cerr != nil {
		err = multierror.Append(err, cerr)
	}

	if rerr := os.Remove(f.cache.Name()); rerr != nil {
		err = multierror.Append(err, rerr)
	}

	return err
}

type S3FileSystemOptions struct {
	Bucket  string
	Session *session.Session
}

type S3FileSystem struct {
	bucket     *string
	downloader *s3manager.Downloader
	uploader   *s3manager.Uploader
}

func NewS3FileSystem(options S3FileSystemOptions) *S3FileSystem {
	return &S3FileSystem{
		bucket:     aws.String(options.Bucket),
		downloader: s3manager.NewDownloader(options.Session),
		uploader:   s3manager.NewUploader(options.Session),
	}
}

func (fs *S3FileSystem) Open(name string) (File, error) {
	cache, err := ioutil.TempFile("", "s3-")
	if err != nil {
		return nil, err
	}

	key := aws.String(name)
	n, err := fs.downloader.Download(cache, &s3.GetObjectInput{
		Bucket: fs.bucket,
		Key:    key,
	})

	if err != nil {
		aerr, ok := err.(awserr.Error)
		if !ok || aerr.Code() != s3.ErrCodeNoSuchKey {
			if rerr := os.Remove(cache.Name()); rerr != nil {
				err = multierror.Append(err, rerr)
			}

			return nil, err
		}
	}

	if _, err := cache.Seek(n, 0); err != nil {
		if rerr := os.Remove(cache.Name()); rerr != nil {
			err = multierror.Append(err, rerr)
		}

		return nil, err
	}

	return &S3File{
		bucket:   fs.bucket,
		key:      key,
		cache:    cache,
		uploader: fs.uploader,
	}, nil
}
