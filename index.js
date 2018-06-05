const AWS = require('ibm-cos-sdk');
const fs = require('fs');
const path = require('path');
const Duplex = require('stream').Duplex;
class CloudObjectStorage {

    constructor(options) {
        let config = {
            apiKeyId: options.apiKeyId,
            endpoint: 's3.eu-geo.objectstorage.softlayer.net',
            ibmAuthEndpoint: 'https://iam.ng.bluemix.net/oidc/token',
            serviceInstanceId: 'crn:v1:bluemix:public:cloud-object-storage:global:a/66656cc0056cb586cab8630ff158cab0:6a0ec026-289d-45df-a92b-262d8762d297::',
            region: "eu-geo" // igual también viene en options
        }

        this.cos = new AWS.S3(config);
    }

    uploadFile(file, bucket, file_path) {
        return this.cos.putObject({
            Bucket: bucket,
            Key: file,
            Body: fs.createReadStream(path.resolve(file_path))
        }).promise();
    }

    uploadFileFromTmp(file, bucket) {
        return this.cos.putObject({
            Bucket: bucket,
            Key: file,
            Body: fs.createReadStream(path.resolve('./.tmp/' + file))
        }).promise();
    }

    async downloadFile(file, bucket) {
        return await new Promise((resolve, reject) => {
            this.cos.getObject({Bucket: bucket, Key: file}, function (error, result) {
                if (error !== null) {
                    reject(error);
                } else {
                    resolve(result.Body);
                }
            })
        })
    }

    async downloadFileStream(file, bucket) {
        return new Promise((resolve, reject) => {
            this.cos.getObject({Bucket: bucket, Key: file}, function (error, data) {
                if (error !== null) {
                    reject(error);
                } else {
                    let stream = new Duplex();
                    stream.push(data.Body);
                    stream.push(null);
                    resolve(stream);
                }
            })
        })
    }

    streamingFile(item, bucket, req, res, next) {
        this.cos.getObject({Bucket: bucket, Key: item}, function (error, data) {
            if (error !== null) {
                res.status(404).send(error);
            } else {
                let stream = new Duplex();
                stream.push(data.Body);
                stream.push(null);
                let buf = [];
                let nb = 0;
                let file = '.tmp/' + item;
                stream.on('data', function(chunk) {
                    buf.push(chunk);
                    nb += chunk.length;
                }).on('end', function() {
                    let buffer = Buffer.concat(buf, nb);
                    fs.writeFile(file, buffer, "binary", function(err, result) {
                        if (err) {
                            return err;
                        } else {
                            fs.stat(file, function(error, stats) {
                                if (error) {
                                    //	1.	Check if the file exists
                                    if (error.code === 'ENOENT') {
                                        // 	->	404 Error if file not found
                                        return res.sendStatus(404);
                                    }
                                    //	2.	IN any other case, just output the full error
                                    return next(err)
                                }
                                let range = req.headers.range;
                                if (!range) {
                                    let err = new Error('Wrong range');
                                    err.status = 416;
                                    //	->	Send the error and stop the request.
                                    //return next(err);
                                }
                                let positions = range.replace(/bytes=/, '').split('-');
                                let start = parseInt(positions[0], 10);
                                let file_size = stats.size;
                                let end = positions[1] ? parseInt(positions[1], 10) : file_size - 1;
                                let chunksize = (end - start) + 1;
                                let head = {
                                    'Content-Range': 'bytes ' + start + '-' + end + '/' + file_size,
                                    'Accept-Ranges': 'bytes',
                                    'Content-Length': chunksize,
                                    'Content-Type': 'video/mp4'
                                }
                                res.writeHead(206, head);
                                let stream_position = {
                                    start: start,
                                    end: end
                                }
                                let stream = fs.createReadStream(file, stream_position);
                                stream.on('open', function() {
                                    stream.pipe(res);
                                });
                                stream.on('error', function(err) {
                                    return next(err);
                                });
                            })
                        }
                    })
                })
            }
        })
    }
}
module.exports = CloudObjectStorage;