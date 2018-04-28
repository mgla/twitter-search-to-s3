NAME=twitter-search-to-s3
LAMBDANAME=$(NAME)

build:
		go-bindata data
		go build -o ${NAME}
zip:
		zip ${NAME}.zip ${NAME}
clean:
	rm -f ${NAME} ${NAME}.zip  bindata.go
deploy: clean build zip updatelambda clean
updatelambda:
	aws lambda update-function-code --function-name ${LAMBDANAME} --zip-file fileb://${NAME}.zip
