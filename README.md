# Description

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
from bitter import easy
wq = easy()
print(wq.users.show(user_name='balkian'))
```


You can also make custom calls to the API through the command line.
e.g. to get the latest 500 tweets by the python software foundation:

```
bitter api statuses/user_timeline --id thepsf --count 500
```


# Examples

The CLI can query the rest API:

```
bitter api <URL endpoint> --parameter VALUE ... | [--tweets | --users] [--max_count MAX_COUNT] [--count COUNT_PER_CALL]
```

For instance:

```
# Get 100 tweets that mentioned Obama after tweet 942689870501302300
bitter api '/search/tweets' --since_id 942689870501302300 --count 100 --q Obama
```

That is equivalent to this call to the api: `api/1.1/searc/tweets?since_id=942689870501302300&count=100&q=Obama`.


The flags `--tweets` and `--users` are optional.
If you use them, bitter will try to intelligently fetch all the tweets/users by using pagination with the API.

For example:

```
# Download 1000 tweets, 100 tweets per call.
bitter api '/search/tweets' --since_id 942689870501302300 --count 100 --q Obama --max_count=1000 --tweets
```

```
# Download all the followers of @balkian
bitter api 'followers/list' --_id balkian --users --max_count -1
```

Note that some reserved words (such as `id`) have to be preceeded by an underscore.
This limitation is imposed by the python-twitter library.

# Configuration format

```
credentials:
- user: "balkian"
  consumer_secret: "xxx"
  consumer_key: "xxx"
  token_key: "xxx"
  token_secret: "xxx"
- user: ....
```

By default, bitter uses '~/.bitter.yaml', but you may choose a different file:

```
python -m bitter --config <config_file> ...
```

Or use an environment variable:

```
export BITTER_CONFIG=$(cat myconfig.yaml)
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
