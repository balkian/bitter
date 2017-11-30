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

from functools import partial

from tqdm import tqdm

from itertools import islice, chain
from contextlib import contextmanager

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
    results = p.imap_unordered(func, source, chunksize=int(1000/numcpus))
    for i in chain.from_iterable(results):
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


def add_user(user, dburi=None, session=None, update=False):
    if not session:
        session = make_session(dburi)

    user = trim_user(user)
    olduser = session.query(User).filter(User.id == user['id'])
    if olduser:
        if not update:
            return
        olduser.delete()
    nuser = User()
    for key, value in user.items():
        setattr(nuser, key, value)
    user = nuser
    if update:
        session.add(user)
        logger.debug('Adding entry')
        entry = session.query(ExtractorEntry).filter(ExtractorEntry.user==user.id).first()
        if not entry:
            entry = ExtractorEntry(user=user.id)
            session.add(entry)
        logger.debug(entry.pending)
        entry.pending = True
        entry.cursor = -1
        session.commit()
    session.close()


def download_entry(wq, entry_id, dburi=None, recursive=False):
    session = make_session(dburi)
    if not session:
        raise Exception("Provide dburi or session")
    logger.info("Downloading entry: %s (%s)" % (entry_id, type(entry_id)))
    entry = session.query(ExtractorEntry).filter(ExtractorEntry.id==entry_id).first()
    user = session.query(User).filter(User.id == entry.user).first()
    download_user(wq, session, user, entry, recursive)
    session.close()


def download_user(wq, session, user, entry=None, recursive=False, max_followers=50000):

    total_followers = user.followers_count

    if total_followers > max_followers:
        entry.pending = False
        logger.info("Too many followers for user: %s" % user.screen_name)
        session.add(entry)
        session.commit()
        return

    if not entry:
        entry = session.query(ExtractorEntry).filter(ExtractorEntry.user==user.id).first() or ExtractorEntry(user=user.id)
    session.add(entry)
    session.commit()

    pending = True
    cursor = entry.cursor
    uid = user.id
    name = user.name

    logger.info("#"*20)
    logger.info("Getting %s - %s" % (uid, name))
    logger.info("Cursor %s" % cursor)
    logger.info("Using account: %s" % wq.name)

    _fetched_followers = 0

    def fetched_followers():
        return session.query(Following).filter(Following.isfollowed==uid).count()

    attempts = 0
    while cursor > 0 or fetched_followers() < total_followers:
        try:
            resp = wq.followers.ids(user_id=uid, cursor=cursor)
        except TwitterHTTPError as ex:
            attempts += 1
            if ex.e.code in (401, ) or attempts > 3:
                logger.info('Not authorized for user: {}'.format(uid))
                entry.errors = ex.message
                break
        if 'ids' not in resp:
            logger.info("Error with id %s %s" % (uid, resp))
            entry.pending = False
            entry.errors = "No ids in response: %s" % resp
            break

        logger.info("New followers: %s" % len(resp['ids']))
        if recursive:
            newusers = get_users(wq, resp)
            for newuser in newusers:
                add_user(session=session, user=newuser)

        if 'ids' not in resp or not resp['ids']:
            logger.info('NO IDS in response')
            break
        for i in resp['ids']:
            existing_user = session.query(Following).\
                            filter(Following.isfollowed == uid).\
                            filter(Following.follower == i).first()
            now = int(time.time())
            if existing_user:
                existing_user.created_at_stamp = now
            else:
                f = Following(isfollowed=uid,
                              follower=i,
                              created_at_stamp=now)
                session.add(f)

        logger.info("Fetched: %s/%s followers" % (fetched_followers(),
                                                  total_followers))
        entry.cursor = resp["next_cursor"]

        session.add(entry)
        session.commit()

    logger.info("Done getting followers for %s" % uid)

    entry.pending = False
    entry.busy = False
    session.add(entry)
    session.commit()

    logger.debug('Entry: {} - {}'.format(entry.user, entry.pending))
    sys.stdout.flush()


def classify_user(id_or_name, screen_names, user_ids):
    try:
        int(id_or_name)
        user_ids.append(id_or_name)
        logger.debug("Added user id")
    except ValueError:
        logger.debug("Added screen_name")
        screen_names.append(id_or_name.split('@')[-1])


def extract(wq, recursive=False, user=None, initfile=None, dburi=None, extractor_name=None):
    signal.signal(signal.SIGINT, signal_handler)

    if not dburi:
        dburi = 'sqlite:///%s.db' % extractor_name

    session = make_session(dburi)
    session.query(ExtractorEntry).update({ExtractorEntry.busy: False})
    session.commit()


    if not (user or initfile):
        logger.info('Using pending users from last session')
    else:
        screen_names = []
        user_ids = []
        if user:
            classify_user(user, screen_names, user_ids)
        elif initfile:
            logger.info("No user. I will open %s" % initfile)
            with open(initfile, 'r') as f:
                for line in f:
                    user = line.strip().split(',')[0]
                    classify_user(user, screen_names, user_ids)

        def missing_user(ix, column=User.screen_name):
            res = session.query(User).filter(column == ix).count() == 0
            if res:
                logger.info("Missing user %s. Count: %s" % (ix, res))
            return res

        screen_names = list(filter(missing_user, screen_names))
        user_ids = list(filter(partial(missing_user, column=User.id_str), user_ids))
        nusers = []
        logger.info("Missing user ids: %s" % user_ids)
        logger.info("Missing screen names: %s" % screen_names)
        if screen_names:
            nusers = list(get_users(wq, screen_names, by_name=True))
        if user_ids:
            nusers += list(get_users(wq, user_ids, by_name=False))

        for i in nusers:
            add_user(dburi=dburi, user=i)

    total_users = session.query(sqlalchemy.func.count(User.id)).scalar()
    logger.info('Total users: {}'.format(total_users))

    de = partial(download_entry, wq, dburi=dburi)
    pending = pending_entries(dburi)
    session.close()

    for i in tqdm(parallel(de, pending), desc='Downloading users', total=total_users):
        logger.info("Got %s" % i)


def pending_entries(dburi):
    session = make_session(dburi)
    while True:
        candidate, entry = session.query(User, ExtractorEntry).\
                        filter(ExtractorEntry.user == User.id).\
                        filter(ExtractorEntry.pending == True).\
                        filter(ExtractorEntry.busy == False).\
                        order_by(User.followers_count).first()
        if candidate:
            entry.busy = True
            session.add(entry)
            session.commit()
            yield int(entry.id)
            continue
        if session.query(ExtractorEntry).\
            filter(ExtractorEntry.busy == True).count() > 0:
            time.sleep(1)
            continue
        logger.info("No more pending entries")
        break
    session.close()

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
    tweet = None
    if update or not cached:
        tweet = get_tweet(wq, tweetid)
        js = json.dumps(tweet, indent=2)
    if write:
        if tweet:
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

def download_timeline(wq, user):
    return wq.statuses.user_timeline(id=user)


def consume_feed(func, *args, **kwargs):
    '''
    Get all the tweets using pagination and a given method.
    It can be controlled with the `count` parameter.

    If count < 0 => Loop until the whole feed is consumed.
    If count == 0 => Only call the API once, with the default values.
    If count > 0 => Get count tweets from the feed.
    '''
    remaining = int(kwargs.pop('count', 0))
    consume = remaining < 0
    limit = False

    # Simulate a do-while by updating the condition at the end
    while not limit:
        if remaining > 0:
            kwargs['count'] = remaining
        resp = func(*args, **kwargs)
        if not resp:
            return
        for t in resp:
            yield t
        if consume:
            continue
        remaining -= len(resp)
        max_id = min(s['id'] for s in func(*args, **kwargs)) - 1
        kwargs['max_id'] = max_id
        limit = remaining <= 0
