package main

import (
	"net/url"
	"os"
	"fmt"
	"encoding/json"
	"bytes"

	"github.com/ChimeraCoder/anaconda"
	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Config struct representation of yaml asset file
type Config struct {
	Units []string
	Fractions []string
}

var (
	searchTerm        = getenv("SEARCH_TERM")
	s3bucket          = getenv("S3_BUCKET")
	s3prefix          = getenvf("S3_PREFIX", "")
	s3region          = getenv("S3_REGION")
	consumerKey       = getenv("TWITTER_CONSUMER_KEY")
	consumerSecret    = getenv("TWITTER_CONSUMER_SECRET")
	accessToken       = getenv("TWITTER_ACCESS_TOKEN")
	accessTokenSecret = getenv("TWITTER_ACCESS_TOKEN_SECRET")
	platform          = getenvf("platform", "dev")
	log = &logger{logrus.New()}
	config Config
)

func getenvf(key, fallback string) string {
	res := os.Getenv(key)
	if res == "" {
		return fallback
	}
	return res
}

func getenv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic("did you forget your keys? " + name)
	}
	return v
}

func searchTwitter() {
	anaconda.SetConsumerKey(consumerKey)
	anaconda.SetConsumerSecret(consumerSecret)
	log.SetLevel(logrus.InfoLevel)
	api := anaconda.NewTwitterApi(accessToken, accessTokenSecret)
	api.SetLogger(log)
	
	// Create a single AWS session (we can re use this if we're uploading many files)
	s, err := session.NewSession(&aws.Config{Region: aws.String(s3region)})
	if err != nil {
		log.Fatal(err)
	}

	v := url.Values{}
	v.Set("count", "100")
	searchRes, err := api.GetSearch(searchTerm, v)
	if err != nil {
		log.Error(fmt.Sprintf("Error: %s", err))
	}
	s3session := s3.New(s)
	for _ , tweet := range searchRes.Statuses {
		json, _ := json.Marshal(tweet)
		s3key := s3prefix + tweet.IdStr + ".json"
		log.Info(fmt.Sprintf("Check if tweet %s already in bucket", tweet.IdStr))
		_, err := s3session.HeadObject(&s3.HeadObjectInput{
			Bucket:               aws.String(s3bucket),
			Key:                  aws.String(s3key),
		})
		upload := false
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchBucket:
					log.Fatal(fmt.Sprintf("Bucket does not exist: %s", err))
				case s3.ErrCodeNoSuchKey:
					log.Info(fmt.Sprintf("Object does not exist in bucket %s", err))
					upload = true
				case "NotFound":  // this seems to be an undocumented aws-sdk behaviour..
					log.Info("Object does not exist in bucket. Upload.")
					log.Debug(fmt.Sprintf("Error: %s", err))
					upload = true
				default:
					log.Fatal(fmt.Sprintf("Unknown AWS error: %s", aerr.Code()))
				}
			} else {
				log.Fatal(fmt.Sprintf("Unknown error: %s", err))
			}
		} else {
			log.Info(fmt.Sprintf("File exists, won't upload: %s/%s", s3bucket, s3key))
		}
		if upload {
			log.Info(fmt.Sprintf("Write s3://%s/%s", s3bucket, s3key))
			_, err = s3session.PutObject(&s3.PutObjectInput{
				Bucket:               aws.String(s3bucket),
				Body:                 bytes.NewReader(json),
				Key:                  aws.String(s3key),
				ServerSideEncryption: aws.String("AES256"),
			})
			if err != nil {
				log.Fatal(fmt.Sprintf("Fatal: %s", err))
			}
		}
	}
}

func main() {
	if (platform == "lambda") {
		lambda.Start(searchTwitter)
	} else {
		searchTwitter()
	}
}

type logger struct {
	*logrus.Logger
}

func (log *logger) Critical(args ...interface{})                 { log.Error(args...) }
func (log *logger) Criticalf(format string, args ...interface{}) { log.Errorf(format, args...) }
func (log *logger) Notice(args ...interface{})                   { log.Info(args...) }
func (log *logger) Noticef(format string, args ...interface{})   { log.Infof(format, args...) }
