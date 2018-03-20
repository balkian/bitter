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


## Adding credentials

```
bitter --config <YOUR CONFIGURATION FILE> credentials add
```

You can specify the parameters in the command or let the command line guide you through the process.

# Examples

## Downloading a list of tweets

Bitter can download tweets from a list of tweets in a CSV file.
The result is stored as individual json files in your folder of choice.
You can even specify the column number for tweet ids.
Bitter will not try to download 

```
Usage: bitter tweet get_all [OPTIONS] TWEETSFILE

  Download tweets from a list of tweets in a CSV file. The result is stored
  as individual json files in your folder of choice.

Options:
  -f, --folder TEXT
  -d, --delimiter TEXT
  -h, --header          Discard the first line (use it as a header)
  -q, --quotechar TEXT
  -c, --column INTEGER
  --help                Show this message and exit.

```

For instance, this will download `tweet_ids.csv` in the `tweet_info` folder:

```
bitter tweet get_all -f tweet_info tweet_ids.csv
```

## Downloading a list of users

Bitter downloads users and tweets in a similar way:

```
Usage: bitter users get_all [OPTIONS] USERSFILE

  Download users from a list of user ids/screen names in a CSV file. The
  result is stored as individual json files in your folder of choice.

Options:
  -f, --folder TEXT
  -d, --delimiter TEXT
  -h, --header          Discard the first line (use it as a header)
  -q, --quotechar TEXT
  -c, --column INTEGER
  --help                Show this message and exit.
```

The only difference is that users can be downloaded via `screen_name` or `user_id`.
This method does not try to resolve screen names to user ids, so users may be downloaded more than once if they appear in both ways.

## Downloading a stream

```
Usage: bitter stream get [OPTIONS]

Options:
  -l, --locations TEXT
  -t, --track TEXT
  -f, --file TEXT       File to store the stream of tweets. Default: standard output
  -p, --politelyretry   Politely retry after a hangup/connection error
  --help                Show this message and exit.
```

```
bitter --config .bitter.yaml stream get 
```
python -m bitter.cli --config .bitter.yaml api '/search/tweets' --result_type recent --q 'bitter OR #bitter OR @bitter' --tweet_mode extended --tweets --max_count 5000 >> mytweets.jsonlines


## REST queries

In newer versions of bitter, individual methods to download tweets/users using the REST API are being replaced with a generic method to call the API.

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
