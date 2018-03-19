python -m bitter.cli --config .bitter.yaml api '/search/tweets' --result_type recent --q 'bitter OR #bitter OR @bitter' --tweet_mode extended --tweets --max_count 5000 >> mytweets.jsonlines
