from __future__ import print_function

import logging
import time
import json

import signal
import sys
import sqlalchemy
import os
import multiprocessing
from multiprocessing.pool import ThreadPool

from tqdm import tqdm

from itertools import islice, chain
from contextlib import contextmanager

try:
    from itertools import izip_longest
except ImportError:
    from itertools import zip_longest

from collections import Counter

from builtins import map, filter

from twitter import TwitterHTTPError

from bitter.models import Following, User, ExtractorEntry, make_session

from bitter import config

logger = logging.getLogger(__name__)


def signal_handler(signal, frame):
    logger.info('You pressed Ctrl+C!')
    sys.exit(0)

def chunk(iterable, n):
    it = iter(iterable)
    return iter(lambda: tuple(islice(it, n)), ())

def parallel(func, source, chunksize=1, numcpus=multiprocessing.cpu_count()):
    source = chunk(source, chunksize)
    p = ThreadPool(numcpus*2)
    for i in chain.from_iterable(p.imap_unordered(func, source, int(1000/numcpus))):
        yield i

def get_credentials_path(credfile=None):
    if not credfile:
        if config.CREDENTIALS:
            credfile = config.CREDENTIALS
        else:
            raise Exception('No valid credentials file')
    return os.path.expanduser(credfile)

@contextmanager
def credentials_file(credfile, *args, **kwargs):
    p = get_credentials_path(credfile)
    with open(p, *args, **kwargs) as f:
        yield f

def iter_credentials(credfile=None):
    with credentials_file(credfile) as f:
        for l in f:
            yield json.loads(l.strip())

def get_credentials(credfile=None, inverse=False, **kwargs):
    creds = []
    for i in iter_credentials(credfile):
        matches = all(map(lambda x: i[x[0]] == x[1], kwargs.items()))
        if matches and not inverse:
            creds.append(i)
        elif inverse and not matches:
            creds.append(i)
    return creds

def create_credentials(credfile=None):
    credfile = get_credentials_path(credfile)
    with credentials_file(credfile, 'a'):
        pass

def delete_credentials(credfile=None, **creds):
    tokeep = get_credentials(credfile, inverse=True, **creds)
    with credentials_file(credfile, 'w') as f:
        for i in tokeep:
            f.write(json.dumps(i))
            f.write('\n')

def add_credentials(credfile=None, **creds):
    exist = get_credentials(credfile, **creds)
    if not exist:
        with credentials_file(credfile, 'a') as f:
            f.write(json.dumps(creds))
            f.write('\n')


def get_hashtags(iter_tweets, best=None):
    c = Counter()
    for tweet in iter_tweets:
        c.update(tag['text'] for tag in tweet.get('entities', {}).get('hashtags', {}))
    return c

def read_file(filename, tail=False):
    with open(filename) as f:
        while True:
            line = f.readline()
            if line not in (None, '', '\n'):
                tweet = json.loads(line.strip())
                yield tweet
            else:
                if tail:
                    time.sleep(1)
                else:
                    return
    

def get_users(wq, ulist, by_name=False, queue=None, max_users=100):
    t = 'name' if by_name else 'uid'
    logger.debug('Getting users by {}: {}'.format(t, ulist))
    ilist = iter(ulist)
    while True:
        userslice = ",".join(str(i) for i in islice(ilist, max_users))
        if not userslice:
            break
        try:
            if by_name:
                resp = wq.users.lookup(screen_name=userslice)
            else:
                resp = wq.users.lookup(user_id=userslice)
        except TwitterHTTPError as ex:
            if ex.e.code in (404,):
                resp = []
            else:
                raise
        if not resp:
            logger.debug('Empty response')
        for user in resp:
            user = trim_user(user)
            if queue:
                queue.put(user)
            else:
                yield user

def trim_user(user):
    if 'status' in user:
        del user['status']
    if 'follow_request_sent' in user:
        del user['follow_request_sent']
    if 'created_at' in user:
        ts = time.strftime('%s', time.strptime(user['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
        user['created_at_stamp'] = ts
        del user['created_at']
    user['entities'] = json.dumps(user['entities'])
    return user


def add_user(session, user, enqueue=False):
    user = trim_user(user)
    olduser = session.query(User).filter(User.id==user['id'])
    if olduser:
        olduser.delete()
    user = User(**user)
    session.add(user)
    if extract:
        logger.debug('Adding entry')
        entry = session.query(ExtractorEntry).filter(ExtractorEntry.user==user.id).first()
        if not entry:
            entry = ExtractorEntry(user=user.id)
            session.add(entry)
        logger.debug(entry.pending)
        entry.pending = True
        entry.cursor = -1
        session.commit()


# TODO: adapt to the crawler
def extract(wq, recursive=False, user=None, initfile=None, dburi=None, extractor_name=None):
    signal.signal(signal.SIGINT, signal_handler)

    w = wq.next()
    if not dburi:
        dburi = 'sqlite:///%s.db' % extractor_name

    session = make_session(dburi)

    screen_names = []
    user_ids = []

    def classify_user(id_or_name):
        try:
            int(user)
            user_ids.append(user)
            logger.info("Added user id")
        except ValueError:
            logger.info("Added screen_name")
            screen_names.append(user.split('@')[-1])

    if user:
        classify_user(user)

    elif initfile:
        logger.info("No user. I will open %s" % initfile)
        with open(initfile, 'r') as f:
            for line in f:
                user = line.strip().split(',')[0]
                classify_user(user)
    else:
        logger.info('Using pending users from last session')


    nusers = list(get_users(wq, screen_names, by_name=True))
    if user_ids:
        nusers += list(get_users(wq, user_ids, by_name=False))

    for i in nusers:
        add_user(session, i, enqueue=True)

    total_users = session.query(sqlalchemy.func.count(User.id)).scalar()
    logger.info('Total users: {}'.format(total_users))
    def pending_entries():
        pending = session.query(ExtractorEntry).filter(ExtractorEntry.pending == True).count()
        logger.info('Pending: {}'.format(pending))
        return pending

    while pending_entries() > 0:
        logger.info("Using account: %s" % w.name)
        candidate, entry = session.query(User, ExtractorEntry).\
                           filter(ExtractorEntry.user == User.id).\
                           filter(ExtractorEntry.pending == True).\
                           order_by(User.followers_count).first()
        if not candidate:
            break
        pending = True
        cursor = entry.cursor
        uid = candidate.id
        uobject = session.query(User).filter(User.id==uid).first()
        name = uobject.screen_name if uobject else None

        logger.info("#"*20)
        logger.info("Getting %s - %s" % (uid, name))
        logger.info("Cursor %s" % cursor)
        logger.info("Pending: %s/%s" % (session.query(ExtractorEntry).filter(ExtractorEntry.pending==True).count(), total_users))
        try:
            resp = wq.followers.ids(user_id=uid, cursor=cursor)
        except TwitterHTTPError as ex:
            if ex.e.code in (401, ):
                logger.info('Not authorized for user: {}'.format(uid))
                resp = {}
        if 'ids' in resp:
            logger.info("New followers: %s" % len(resp['ids']))
            if recursive:
                newusers = get_users(wq, resp)
                for user in newusers:
                    add_user(session, newuser, enqueue=True)
            for i in resp['ids']:
                existing_user = session.query(Following).\
                                filter(Following.isfollowed==uid).\
                                filter(Following.follower==i).first()
                now = int(time.time())
                if existing_user:
                    existing_user.created_at_stamp = now
                else:
                    f = Following(isfollowed=uid,
                                  follower=i,
                                  created_at_stamp=now)
                    session.add(f)

            total_followers = candidate.followers_count
            fetched_followers = session.query(Following).filter(Following.isfollowed==uid).count()
            logger.info("Fetched: %s/%s followers" % (fetched_followers,
                                                      total_followers))
            cursor = resp["next_cursor"]
            if cursor > 0:
                pending = True
                logger.info("Getting more followers for %s" % uid)
            else:
                logger.info("Done getting followers for %s" % uid)
                cursor = -1
                pending = False
        else:
            logger.info("Error with id %s %s" % (uid, resp))
            pending = False

        entry.pending = pending
        entry.cursor = cursor
        logger.debug('Entry: {} - {}'.format(entry.user, entry.pending))

        session.add(candidate)
        session.commit()

        sys.stdout.flush()


def get_tweet(c, tid):
    return c.statuses.show(id=tid)

def search_tweet(c, query):
    return c.search.tweets(q=query)

def user_timeline(c, query):
    try:
        return c.statuses.user_timeline(user_id=int(query))
    except ValueError:
        return c.statuses.user_timeline(screen_name=query)

def get_user(c, user):
    try:
        int(user)
        return c.users.lookup(user_id=user)[0]
    except ValueError:
        return c.users.lookup(screen_name=user)[0]

def download_tweet(wq, tweetid, write=True, folder="downloaded_tweets", update=False):
    cached = cached_tweet(tweetid, folder)
    newtweet = None
    if update or not cached:
        newtweet = get_tweet(wq, tweetid)
        js = json.dumps(tweet, indent=2)
    if write:
        if newtweet:
            write_tweet_json(js, folder)
    else:
        print(js)


def cached_tweet(tweetid, folder):
    tweet = None
    file = os.path.join(folder, '%s.json' % tweetid)
    if os.path.exists(file) and os.path.isfile(file):
        try:
            # print('%s: Tweet exists' % tweetid)
            with open(file) as f:
                tweet = json.load(f)
        except Exception as ex:
            logger.error('Error getting cached version of {}: {}'.format(tweetid, ex))
    return tweet

def write_tweet_json(js, folder):
    tweetid = js['id']
    file = tweet_file(tweetid, folder)
    if not os.path.exists(folder):
        os.makedirs(folder)
    with open(file, 'w') as f:
        json.dump(js, f, indent=2)
        logger.info('Written {} to file {}'.format(tweetid, file))

def tweet_file(tweetid, folder):
    return os.path.join(folder, '%s.json' % tweetid)

def tweet_fail_file(tweetid, folder):
    failsfolder = os.path.join(folder, 'failed')
    if not os.path.exists(failsfolder):
        os.makedirs(failsfolder)
    return os.path.join(failsfolder, '%s.failed' % tweetid)

def tweet_failed(tweetid, folder):
    return os.path.isfile(tweet_fail_file(tweetid, folder))

def download_tweets(wq, tweetsfile, folder, update=False, retry_failed=False, ignore_fails=True):
    def filter_line(line):
        tweetid = int(line)
        # print('Checking {}'.format(tweetid))
        if (cached_tweet(tweetid, folder) and not update) or (tweet_failed(tweetid, folder) and not retry_failed):
            yield None
        else:
            yield line

    def print_result(res):
        tid, tweet = res
        if tweet:
            try:
                write_tweet_json(tweet, folder=folder)
                yield 1
            except Exception as ex:
                logger.error('%s: %s' % (tid, ex))
                if not ignore_fails:
                    raise
        else:
            logger.info('Tweet not recovered: {}'.format(tid))
            with open(tweet_fail_file(tid, folder), 'w') as f:
                print('Tweet not found', file=f)
            yield -1

    def download_batch(batch):
        tweets = wq.statuses.lookup(_id=",".join(batch), map=True)['id']
        return tweets.items()

    with open(tweetsfile) as f:
        lines = map(lambda x: x.strip(), f)
        lines_to_crawl = filter(lambda x: x is not None, tqdm(parallel(filter_line, lines), desc='Total lines'))
        tweets = parallel(download_batch, lines_to_crawl, 100)
        for res in tqdm(parallel(print_result, tweets), desc='Queried'):
            pass
