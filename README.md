#Description
There are two parts to bitter.
First of all, it is a wrapper over Python twitter that adds support for several Twitter API credentials (e.g. authorizing the same app with different user accounts).
Secondly, it is a command line tool to automate several actions (e.g. downloading user networks) using the wrapper.

# Instructions

In the command line:

    python -m bitter --help

or

    bitter --help


Programmatically:

```python
from bitter.crawlers import TwitterQueue
wq = TwitterQueue.from_credentials()
print(wq.users.show(user_name='balkian'))
```

# Credentials format

```
{"user": "balkian", "consumer_secret": "xxx", "consumer_key": "xxx", "token_key": "xxx", "token_secret": "xxx"}
```

By default, bitter uses '~/.bitter-credentials.json', but you may choose a different file:

```
python -m bitter -c <credentials_file> ...
```

# Server
To add more users to the credentials file, you may run the builtin server, with the consumer key and secret of your app:

```
python -m bitter server <consumer_key> <consumer_secret>
```

If you get an error about missing dependencies, install the extra dependencies for the server. e.g.:

```
pip install bitter[web]
```

Make sure the callback url of your app is set to http://127.0.0.1:5000/callback_url/

# Notice
Please, use according to Twitter's Terms of Service

# TODO

* Tests
* Docs
