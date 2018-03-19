Scripts to process jsonlines

To get the jsonlines file, you can use the streaming API or the search api, like so:

```
python -m bitter.cli --config .bitter.yaml api '/search/tweets' --result_type recent --q 'bitter OR #bitter OR @bitter' --tweet_mode extended --tweets --max_count 5000 >> mytweets.jsonlines
```

To keep track of the query that generated the file, you can save the command in a text file.
For instance, the example above is also in `example_query.sh`.
