# s3uploader

**Note: The development was done using Ubuntu, so you may find issues when running on Windows. Please do not hesitate and let me know of any issues/problems/etc you encounter and will fix it ASAP.**

## Configuring application

### Configuration
The monitored folder is read from environment variable `MONITORED_FOLDER`. Config file must be placed in `config/config.json` under `MONITORED_FOLDER`.

### Logging
s3uploder uses standard Python logging configured via `config.json` file. Currently there are two loggers configured: one for console an done for file, rotated each day, keeping the last five logs. Logs are stored in `logs/app.log` under monitored folder.

### Important configuration keys

AWs Credentials
```
    "aws_key": "EXAMPLE_KEY",
    "aws_secret": "EXAMPLE_SECRET",
```

AWS does not allow `/`  in bucket names so we have an additional key to create a hierarchy. `bucket-path` will be concatenated with each media file name. Note the trailing `/`
```
    "bucket": "flx-videobooth",
    "bucket-path": "videos/",
```

Note there is no `.` (dot)
```
    "watch-extension": "json",
```

This is the log level for the console
```
    "log-config": {
        "version": 1,
        "root": {
            "level": "DEBUG",
```

This is the log file name. It will be created under `MONITORED_FOLDER`
```
                "filename": "logs/app.log",
```

## Run application


Clone the repository:
```
$ git clone git@github.com:dedosmedia/s3uploader.git
```

Build the docker image:
```
$ cd s3uploader
$ docker build -t s3uploader .
```

Run the image:
```
$ docker run -i -e "MONITORED_FOLDER=/opt/mf" -v <PATH TO  MONITORED FOLDER ON YOUR HOST>:/opt/mf s3uploader
```

Run the image with always restart:
```
$ docker run -i -d --restart unless-stopped -e "MONITORED_FOLDER=/opt/mf" -v "C:/Path/to/upload":/opt/mf s3uploader
```

Enjoy :-)

