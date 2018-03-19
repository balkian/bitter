from __future__ import print_function

import click
import json
import os
import logging
import time
import sqlalchemy.types
import threading
import sqlite3
from tqdm import tqdm

from sqlalchemy import exists

from bitter import utils, models, crawlers
from bitter import config as bconf
from bitter.models import make_session, User, ExtractorEntry, Following

import sys
if sys.version_info <= (3, 0):
    from contextlib2 import ExitStack
else:
    from contextlib import ExitStack
    


logger = logging.getLogger(__name__)

@click.group()
@click.option("--verbose", is_flag=True)
@click.option("--logging_level", required=False, default='WARN')
@click.option('--config', show_default=True, default=bconf.CONFIG_FILE)
@click.option('--credentials', show_default=True, help="DEPRECATED: If specified, these credentials will be copied to the configuratation file.", default=bconf.CREDENTIALS)
@click.pass_context
def main(ctx, verbose, logging_level, config, credentials):
    logging.basicConfig(level=getattr(logging, logging_level))
    ctx.obj = {}
    ctx.obj['VERBOSE'] = verbose
    bconf.CONFIG_FILE = config
    bconf.CREDENTIALS = credentials
    if os.path.exists(utils.get_config_path(credentials)):
      utils.copy_credentials_to_config(credentials, config)

@main.group()
@click.pass_context 
def tweet(ctx):
    pass

@tweet.command('get')
@click.option('-w', '--write', is_flag=True, default=False)
@click.option('-f', '--folder', default="tweets")
@click.option('-u', '--update', help="Update the file even if the tweet exists", is_flag=True, default=False)
@click.argument('tweetid')
def get_tweet(tweetid, write, folder, update):
    wq = crawlers.TwitterQueue.from_config(bconf.CONFIG_FILE)
    utils.download_tweet(wq, tweetid, write, folder, update)
        
@tweet.command('get_all')
@click.argument('tweetsfile', 'File with a list of tweets to look up')
@click.option('-f', '--folder', default="tweets")
@click.pass_context
def get_tweets(ctx, tweetsfile, folder):
    wq = crawlers.TwitterQueue.from_config(bconf.CONFIG_FILE)
    utils.download_tweets(wq, tweetsfile, folder)

@tweet.command('search')
@click.argument('query')
@click.pass_context 
def search(ctx, query):
    wq = crawlers.TwitterQueue.from_config(bconf.CONFIG_FILE)
    t = utils.search_tweet(wq, query)
    print(json.dumps(t, indent=2))

@tweet.command('timeline')
@click.argument('user')
@click.pass_context 
def timeline(ctx, user):
    wq = crawlers.TwitterQueue.from_config(bconf.CONFIG_FILE)
    t = utils.user_timeline(wq, user)
    print(json.dumps(t, indent=2))

@main.group()
@click.pass_context
def users(ctx):
    pass

@users.command('list')
@click.option('--db', required=True, help='Database of users.')
@click.pass_context
def list_users(ctx, db):
    dburl = 'sqlite:///{}'.format(db)
    session = make_session(dburl)
    for i in session.query(User):
        print(i.screen_name)
        for j in i.__dict__:
            print('\t{}: {}'.format(j, getattr(i,j)))

@users.command('get')
@click.argument('user')
@click.option('-w', '--write', is_flag=True, default=False)
@click.option('-f', '--folder', default="users")
@click.option('-u', '--update', help="Update the file even if the user exists", is_flag=True, default=False)
def get_user(user, write, folder, update):
    wq = crawlers.TwitterQueue.from_config(bconf.CONFIG_FILE)
    if not write:
        u = utils.get_user(wq, user)
        js = json.dumps(u, indent=2)
        print(js)
        return
    if not os.path.exists(folder):
        os.makedirs(folder)
    file = os.path.join(folder, '%s.json' % user)
    if not update and os.path.exists(file) and os.path.isfile(file):
        print('User exists: %s' % user)
        return
    with open(file, 'w') as f:
        u = utils.get_user(wq, user)
        js = json.dumps(u, indent=2)
        print(js, file=f)

@users.command('get_all')
@click.argument('usersfile', 'File with a list of users to look up')
@click.option('-f', '--folder', default="users")
@click.pass_context
def get_users(ctx, usersfile, folder):
    with open(usersfile) as f:
        for line in f:
            uid = line.strip()
            ctx.invoke(get_user, folder=folder, user=uid, write=True)

@users.command('crawl')
@click.option('--db', required=True, help='Database to save all users.')
@click.option('--skip', required=False, default=0, help='Skip N lines from the file.')
@click.option('--until', required=False, type=str, default=0, help='Skip all lines until ID.')
@click.option('--threads', required=False, type=str, default=20, help='Number of crawling threads.')
@click.argument('usersfile', 'File with a list of users to look up')
@click.pass_context
def crawl_users(ctx, usersfile, skip, until, threads, db):
    global dburl, ids_queue, skipped, enqueued, collected, lastid, db_lock

    if '://' not in db:
        dburl = 'sqlite:///{}'.format(db)
        db_lock = threading.Lock()
    else:
        dburl = db
        def db_lock():
            return ExitStack()


    wq = crawlers.TwitterQueue.from_config(bconf.CONFIG_FILE)
    logger.info('Starting Network crawler with {} threads and {} credentials.'.format(threads,
                                                                                      len(wq.queue)))

    ids_queue = queue.Queue(1000)
    skipped = skip
    enqueued = 0
    collected = 0
    statslock = threading.Lock()
    lastid = -1

    def fill_queue():
        global enqueued, skipped
        with open(usersfile, 'r') as f:
            sqlite = sqlite3.connect(db)
            engine = sqlalchemy.create_engine(dburl)
            def user_filter(x):
                global skipped, dburl
                # keep = data['users'].find_one(id=x) is None
                #keep = not session.query(exists().where(User.id == x)).scalar()
                # keep = session.engine.execute
                keep = not list(engine.execute('SELECT 1 from users where id=\'%s\'' % x))

                if not keep:
                    skipped += 1
                return keep
            for i in range(skip):
                next(f)
            ilist = map(lambda x: x.strip(), f)
            logger.info('Skipping until {}'.format(until))
            if not skip and until:
                for uid in ilist:
                    if uid == until:
                        break
                    else:
                        skipped += 1
            ilist = filter(user_filter, ilist)
            for uid in ilist:
                ids_queue.put(uid)
                enqueued += 1
        for i in range(threads):
            ids_queue.put(None)

    def consume_queue():
        global dburl, collected, ids_queue, lastid
        local_collected = 0
        logging.debug('Consuming!')
        session = make_session(dburl)
        q_iter = iter(ids_queue.get, None)
        for user in utils.get_users(wq, q_iter):
            dbuser = User(**user)
            session.add(dbuser)
            local_collected += 1
            with statslock:
                collected += 1
                lastid = user['id']
            if local_collected % 100 == 0:
                with db_lock:
                    session.commit()
        session.commit()
        logger.debug('Done consuming')

    filler = threading.Thread(target=fill_queue)
    filler.start()
    consumers = [threading.Thread(target=consume_queue) for i in range(threads)]
    logging.debug('Starting consumers')
    for c in consumers:
        c.start()
    logging.debug('Joining filler')
    counter = 0
    speed = 0
    lastcollected = collected
    while True:
        filler.join(1)
        logger.info('########\n'
                    '   Collected: {}\n'
                    '   Speed: ~ {} profiles/s\n'
                    '   Skipped: {}\n'
                    '   Enqueued: {}\n'
                    '   Queue size: {}\n'
                    '   Last ID: {}'.format(collected, speed, skipped, enqueued, ids_queue.qsize(), lastid))
        if not filler.isAlive():
            if all(not i.isAlive() for i in consumers):
                break
            else:
                time.sleep(1)
        counter += 1
        if counter % 10 == 0:
            speed = (collected-lastcollected)/10
            with statslock:
                lastcollected = collected
            
    logger.info('Done!')

@main.group('extractor')
@click.pass_context
@click.option('--db', required=True, help='Database of users.')
def extractor(ctx, db):
    if '://' not in db:
        db = 'sqlite:///{}'.format(db)
    ctx.obj['DBURI'] = db
    ctx.obj['SESSION'] = make_session(db)


@extractor.command('status')
@click.option('--with_followers', is_flag=True, default=False)
@click.option('--with_not_pending', is_flag=True, default=False)
@click.pass_context
def status_extractor(ctx, with_followers, with_not_pending):
    session = ctx.obj['SESSION']
    entries = session.query(ExtractorEntry)
    if not with_not_pending:
        entries = entries.filter(ExtractorEntry.pending==True)
    for i in entries:
        print(i.id)
        for j in i.__dict__:
            print('\t{}: {}'.format(j, getattr(i,j)))
    followers = session.query(Following)
    print('Followers count: {}'.format(followers.count()))
    if(with_followers):
        for i in followers:
            print(i.id)
            for j in i.__dict__:
                print('\t{}: {}'.format(j, getattr(i,j)))

@extractor.command('network')
@click.option('--as_json', is_flag=True, default=False)
@click.pass_context
def network_extractor(ctx, as_json):
    session = ctx.obj['SESSION']
    followers = session.query(Following)
    follower_map = []
    for i in followers:
        if not as_json:
            print('{} -> {}'.format(i.follower, i.isfollowed))
        else:
            follower_map.append({'source_id': i.follower,
                                 'target_id': i.isfollowed,
                                 'following': True})
    if as_json:
        import json
        print(json.dumps(follower_map, indent=4))
    

@extractor.command('users')
@click.pass_context
def users_extractor(ctx):
    session = ctx.obj['SESSION']
    users = session.query(User)
    import json
    for i in users:
        # print(json.dumps(i.as_dict(), indent=4))
        dd = i.as_dict()
        print(json.dumps(dd, indent=4))


@extractor.command()
@click.option('--recursive', is_flag=True, help='Get following/follower/info recursively.', default=False)
@click.option('-u', '--user', default=None)
@click.option('-n', '--name', show_default=True, default='extractor')
@click.option('-i', '--initfile', required=False, default=None, help='List of users to load')
@click.pass_context
def extract(ctx, recursive, user, name, initfile):
    print(locals())
    wq = crawlers.TwitterQueue.from_config(bconf.CONFIG_FILE)
    dburi = ctx.obj['DBURI']
    utils.extract(wq,
                  recursive=recursive,
                  user=user,
                  dburi=dburi,
                  initfile=initfile,
                  extractor_name=name)

@extractor.command('reset')
@click.pass_context
def reset_extractor(ctx):
    wq = crawlers.TwitterQueue.from_config(bconf.CONFIG_FILE)
    db = ctx.obj['DBURI']
    session = make_session(db)
    session.query(ExtractorEntry).filter(ExtractorEntry.pending==True).update({'pending':False})

@main.command('limits')
@click.argument('url', required=False)
@click.pass_context
def get_limits(ctx, url):
    wq = crawlers.TwitterQueue.from_config(bconf.CONFIG_FILE)
    total = {}
    for worker in wq.queue:
        resp = worker.client.application.rate_limit_status()
        print('#'*20)
        print(worker.name)
        if url:
            limit = 'NOT FOUND'
            print('URL is: {}'.format(url))
            cat = url.split('/')[1]
            if cat in resp['resources']:
                limit = resp['resources'][cat].get(url, None) or resp['resources'][cat]
            else:
                print('Cat {} not found'.format(cat))
                continue
            for k in limit:
                total[k] = total.get(k, 0) + limit[k]
            print('{}: {}'.format(url, limit))
        else:
            print(json.dumps(resp, indent=2))
    if url:
        print('Total for {}: {}'.format(url, total))



@main.command(context_settings=dict(ignore_unknown_options=True, allow_extra_args=False))
@click.argument('cmd', nargs=1)
@click.option('--tweets', is_flag=True, help='Fetch more tweets using smart pagination. Use --count to control how many tweets to fetch per call, and --max_count to set the number of desired tweets (or -1 to get as many as possible).', type=bool, default=False)
@click.option('--users', is_flag=True, help='Fetch more users using smart pagination. Use --count to control how many users to fetch per call, and --max_count to set the number of desired users (or -1 to get as many as possible).', type=bool, default=False)
@click.argument('api_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def api(ctx, cmd, tweets, users, api_args):
    opts = {}
    mappings = {
        'id': '_id'
    }
    i = iter(api_args)
    for k, v in zip(i, i):
        k = k.replace('--', '')
        if k in mappings:
            k = mappings[k]
        opts[k] = v
    wq = crawlers.TwitterQueue.from_config(bconf.CONFIG_FILE)
    if tweets:
        resp = utils.consume_tweets(wq[cmd], **opts)
    elif users:
        resp = utils.consume_users(wq[cmd], **opts)
    else:
        resp = wq[cmd](**opts)
        print(json.dumps(resp))
        return
    for i in resp:
        print(json.dumps(i))


@main.command('server')
@click.argument('CONSUMER_KEY', required=True)
@click.argument('CONSUMER_SECRET', required=True)
@click.pass_context
def run_server(ctx, consumer_key, consumer_secret):
    bconf.CONSUMER_KEY = consumer_key
    bconf.CONSUMER_SECRET = consumer_secret
    from .webserver import app
    app.run(host='0.0.0.0')

@main.group()
@click.pass_context 
def stream(ctx):
    pass

@stream.command('get')
@click.option('-l', '--locations', default=None)
@click.option('-t', '--track', default=None)
@click.option('-f', '--file', default=None, help='File to store the stream of tweets')
@click.option('-p', '--politelyretry', help='Politely retry after a hangup/connection error', is_flag=True, default=True)
@click.pass_context 
def get_stream(ctx, locations, track, file, politelyretry):
    wq = crawlers.StreamQueue.from_config(bconf.CONFIG_FILE, 1)

    query_args = {}
    if locations:
        query_args['locations'] = locations
    if track:
        query_args['track'] = track
    if not file:
        file = sys.stdout
    else:
        file = open(file, 'a')

    def insist():
        lasthangup = time.time()
        while True:
            if not query_args:
                iterator = wq.statuses.sample()
            else:
                iterator = wq.statuses.filter(**query_args)#"-4.25,40.16,-3.40,40.75")
            try:
              for i in iterator:
                  yield i
              if not politelyretry:
                  return
            except Exception:
                if not politelyretry:
                    raise ex
            thishangup = time.time()
            if thishangup - lasthangup < 60:
                raise Exception('Too many hangups in a row.')
            time.sleep(3)

    for tweet in tqdm(insist()):
        print(json.dumps(tweet), file=file)
    if file != sys.stdout:
        file.close()

@stream.command('read')
@click.option('-f', '--file', help='File to read the stream of tweets from', required=True)
@click.option('-t', '--tail', is_flag=True, help='Keep reading from the file, like tail', type=bool, default=False)
@click.pass_context 
def read_stream(ctx, file, tail):
    for tweet in utils.read_file(file, tail=tail):
        try:
            print(u'{timestamp_ms}- @{screen_name}: {text}'.format(timestamp_ms=tweet['created_at'], screen_name=tweet['user']['screen_name'], text=tweet['text']))
        except (KeyError, TypeError):
            print('Raw tweet: {}'.format(tweet))

@stream.command('tags')
@click.option('-f', '--file', help='File to read the stream of tweets from', required=True)
@click.argument('limit', required=False, default=None, type=int)
@click.pass_context 
def tags_stream(ctx, file, limit):
    c = utils.get_hashtags(utils.read_file(file))
    for count, tag in c.most_common(limit):
        print(u'{} - {}'.format(count, tag))
    

if __name__ == '__main__':
    main()
